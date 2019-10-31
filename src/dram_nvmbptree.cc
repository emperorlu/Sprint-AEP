#include "dram_nvmbptree.h"
#include <assert.h>
#include <cmath>
#include<string.h>

namespace rocksdb{
    int  DrBpPrintCount_ = 0;
    int  DrBpTreeLevel_ = 0;
    // PersistentAllocator* BpNodeNVMAllocator_;
    PersistentAllocator* DrBpValueNVMAllocator_;
    Statistic DrbpStats;

//****************************************/
//*B+树节点结构实现
//****************************************/

void BpNode::InsertAtLeaf(string key, string value)
{
    assert(m_currentSize < DrNVMBp_NODE_CAPACITY);
    int i;
    if(m_currentSize == 0) {
        SetKey(m_currentSize, key.c_str());
        SetCurrentSize(m_currentSize + 1);
        return ;
    }
    for (i = 0; i < m_currentSize; ++i)
    {
        int res = memcmp(key.c_str(), m_key[i], NVM_KeySize);
        if (res <= 0) //找到插入点
        {
           break;
        }
    }

    //后续所有节点后移一个下标,对应指针同步操作
    for (int j = m_currentSize - 1; j >= i; --j)
    {
        // m_key[j + 1] = m_key[j];
        SetKey(j + 1, m_key[j]);
    } 
    SetKey(i, key.c_str());
    // m_currentSize++;
    SetCurrentSize(m_currentSize + 1);
}

void BpNode::InsertIndex(string key, BpNode *_pointer)
{
    int i;
    assert(m_currentSize < DrNVMBp_NODE_CAPACITY);

    if(m_currentSize == 0) {
        SetKey(m_currentSize, key.c_str());
        SetPointer(m_currentSize, _pointer);
        SetCurrentSize(m_currentSize + 1);
        return ;
    }
    for (i = 0; i < m_currentSize; ++i)
    {
        int res = memcmp(key.c_str(), m_key[i], NVM_KeySize);
        if (res <= 0) //找到插入点
        {
            break;
        }
    }
    //后续所有节点后移一个下标,对应指针同步操作
    for (int j = m_currentSize - 1; j >= i; --j)
    {
        SetKey(j + 1, m_key[j]);
        SetPointer(j + 1, m_pointer[j]);
    }
    SetKey(i, key.c_str());
    SetPointer(i, _pointer);
    SetCurrentSize(m_currentSize + 1);

}

// void BpNode::UpdateKey(string keyOld, string keyNew)
// {
//     for (int i = 0; i < m_currentSize; ++i)
//     {
//         if (m_key[i] == keyOld)
//         {
//             m_key[i] = keyNew;
//             return;
//         }
//     }
// }

void BpNode::InsertAtIndex(string key, string value)
{
    int res = memcmp(key.c_str(), m_key[m_currentSize-1], NVM_KeySize);
    if(res > 0)
    {
        SetKey(m_currentSize-1, key.c_str());
        m_pointer[m_currentSize-1]->Insert(key, value);
        if(m_pointer[m_currentSize-1]->IsNeedSplit())
        {
            BpNode* left = nullptr;
            BpNode* right = nullptr;
            m_pointer[m_currentSize-1]->Split(left, right);

            //取left的最大key和left指针插入到当前索引节点
            string left_key(left->GetMaxKey(),NVM_KeyBuf);
            InsertIndex(left_key, left);
        }
        return ;
    }
    for (int i = 0; i < m_currentSize; ++i)
    {
        int res = memcmp(key.c_str(), m_key[i], NVM_KeySize);
        if (res <= 0)
        {
            m_pointer[i]->Insert(key, value);
            if(m_pointer[i]->IsNeedSplit())
            {
                BpNode* left = NULL;
                BpNode* right = NULL;
                m_pointer[i]->Split(left, right);

                //取left的最大key和left指针插入到当前索引节点
                string left_key(left->GetMaxKey(),NVM_KeyBuf);
                InsertIndex(left_key, left);
            }

            break;//操作完成，即可退出
        }
    }
}

void BpNode::Insert(string key, string value) {
    if(IsLeafNode()) {
        InsertAtLeaf(key, value);
    } else {
        InsertAtIndex(key, value);
    }
}

char * BpNode::Get(const std::string& key) {
    for (int i = 0; i < m_currentSize; ++i)
    {
        int res = strncmp(key.c_str(), m_key[i], NVM_KeySize);
        if (res <= 0)
        {
            if(IsLeafNode()) {
                if(res == 0) {
                    string tmp(m_key[i], NVM_KeyBuf);
                    int len = tmp.length();
                    int hot = stoi(tmp.substr(len-7));
                    hot++;
                    tmp.replace(len-7, 7, to_string(hot));
                    memcpy(m_key[i], tmp.c_str(), NVM_KeyBuf);
                    return m_key[i] + NVM_KeySize;
                }
                else {
                    return nullptr;
                }
            } else {
                return m_pointer[i]->Get(key);
            }
        }
    }
    return nullptr;
}

BpNode *BpNode::FindLeafNode(const std::string key) {
    if(IsLeafNode()) {
        return this;
    }
    for (int i = 0; i < m_currentSize; ++i)
    {
        int res = memcmp(key.c_str(), m_key[i], NVM_KeySize);
        if (res <= 0)
        {
            return m_pointer[i]->FindLeafNode(key);
        }
    }
    return NULL;
}

void BpNode::Split(BpNode* &_left, BpNode* &_right)
{
    assert(m_currentSize == DrNVMBp_NODE_CAPACITY);
    bool isLeaf = IsLeafNode();
    BpNode* pNew = new  BpNode;
    pNew->SetNodeType(GetNodeType());
    pNew->SetNext(nullptr);
    pNew->SetPrev(nullptr);

    int leftCount = (int)ceil((DrNVMBp_M_WAY+1)/2.0);
    int rightCount = m_currentSize - leftCount;
    int index = 0;
    while(index < leftCount)
    {
        string key(m_key[index], NVM_KeyBuf); 
        if(isLeaf) {
            pNew->Insert(key, "");
        } else {
            pNew->InsertIndex(key, m_pointer[index]);
        }
        index++;
    }
    while(index < m_currentSize)
    {
        SetKey(index-leftCount, m_key[index]);
        if(!isLeaf) {
            m_pointer[index - leftCount] = m_pointer[index];
        }
		++index;
    }
    //更新m_currentSize值
    // SetCurrentSize(rightCount);
    m_currentSize = rightCount;
    //更新双向链表指针
    if(isLeaf) {
	    if(this->GetPrev() != nullptr)
	    {
		    this->GetPrev()->SetNext(pNew);
	    }
        pNew->SetPrev(this->GetPrev());
        pNew->SetNext(this);
        this->SetPrev(pNew);
    }	
    _right = this;
    _left = pNew;
}

BpNode* BpNode::GetMinLeaveNode()
{
    if(IsLeafNode()) {
	    return this; 
    } else {
        return m_pointer[0]->GetMinLeaveNode();
    }
}

bool BpNode::Search(string key, string &value, BpNode *&_valueNode)
{
    for (int i = 0; i < m_currentSize; ++i)
    {
        DrbpStats.add_node_search();    
        int res = memcmp(key.c_str(), m_key[i], NVM_KeySize);
        if (res == 0) 
        {
            // value = m_value[i];
            value = "";
            _valueNode = this;

            return true;
        }
        else  if (key < m_key[i]) 
        { //进入此逻辑，说明查无此KEY
            break;
        }
    }

    return false;
}


bool BpNode::Search(string key1, string key2, string* result, int& size)
{
    bool bFind = false;
    for (int i = 0; i < m_currentSize; ++i)
    {  
        int res1 = memcmp(key1.c_str(), m_key[i], NVM_KeySize);
        int res2 = memcmp(key2.c_str(), m_key[i], NVM_KeySize);
        if (res1 <= 0 && res2 >=0 )
        {
            // result[size] = m_value[i];
            result[size] = "";
            size++;

            if(!bFind)
            {
                bFind  = true;
            }
        }
    }

    return bFind;
}

// string BpNode::GetValue(char *valuepointer){
//     uint64_t value_point;
//     memcpy(&value_point, valuepointer, sizeof(uint64_t));
//     char *value = (char *)value_point;
//     return string(value, NVM_ValueSize); 
// }

// bool BpNode::GetRange(const std::string key1, const std::string key2, std::vector<std::string> &values, int &findsize, int size) {
//     bool bFinished = false;
//     for (int i = 0; i < m_currentSize; ++i)
//     {
//         DrbpStats.add_node_search();    
//         // int res1 = memcmp(key1.c_str(), m_key[i], NVM_KeySize);
//         // if (res1 <= 0)
//         if (key1 <= m_key[i]) 
//         {
//             if(key2.size() != 0 && memcmp(key2.c_str(), m_key[i], NVM_KeySize) < 0) {
//                 return true;
//             }
//             values.push_back(GetValue(m_key[i] + NVM_KeySize));
//             findsize ++;
//             if(findsize >= size) {
//                 return true;
//             }
//         }
//     }
//     return bFinished;
// }

void BpNode::deleteIndex(int i) {
    bool isLeaf = IsLeafNode();
    for(; i < m_currentSize - 1; i ++ ) {
        SetKey(i, m_key[i + 1]);
        // m_key[i] = m_key[i+1];
        if(!isLeaf) {
            SetPointer(i, m_pointer[i + 1]);
            // m_pointer[i] = m_pointer[i+1];
        }
    }
    SetCurrentSize(m_currentSize - 1);
    // m_currentSize--;
}

void BpNode::borrowNext(BpNode *next) {
    bool isLeaf = IsLeafNode();
    SetKey(m_currentSize, next->GetKey(0));
    // m_key[m_currentSize] = next->m_key[0];
    if(!isLeaf) {
        SetPointer(m_currentSize, next->GetPointer(0));
        // m_pointer[m_currentSize] = next->m_pointer[0];
    }
    // SetCurrentSize(m_currentSize + 1);
    m_currentSize++;
    next->deleteIndex(0);

}

void BpNode::borrowPrev(BpNode *prev) {
    bool isLeaf = IsLeafNode();
    string left_key(prev->GetMaxKey(), NVM_KeyBuf);
    if(isLeaf) {
        Insert(left_key, "");
    }
    else {
        string left_key(prev->GetMaxKey(), NVM_KeyBuf);
        InsertIndex(left_key, prev->GetPointer(prev->GetSize() - 1));
    }

    prev->deleteIndex(prev->GetSize() - 1);

}

void BpNode::mergeNext(BpNode *next) {
    bool isLeaf = IsLeafNode();
    assert(m_currentSize  + next->GetSize() < DrNVMBp_NODE_CAPACITY);
    for(int i = 0; i < next->GetSize(); i ++) {
        SetKey(m_currentSize + i, next->GetKey(i));
        // m_key[m_currentSize+1] = next->m_key[0];
        if(!isLeaf) {
            SetPointer(m_currentSize + i, next->GetPointer(i));
            // m_pointer[m_currentSize+1] = next->m_pointer[i];

        }
    }
    // SetCurrentSize(m_currentSize + next->GetSize());
    m_currentSize = m_currentSize + next->GetSize();
}

void BpNode::nodeDelete() {
    if(IsLeafNode()) {
        if(this->GetPrev() != NULL)
        {
            this->GetPrev()->SetNext(this->GetNext());
        }

        if(this->GetNext() != NULL)	 {
            this->GetNext()->SetPrev(this->GetPrev());
        }
    }
}

bool BpNode::DeleteAtLeaf(string key)
{
    for (int i = 0; i < m_currentSize; ++i)
    {
        DrbpStats.add_node_search();    
        int res = memcmp(key.c_str(), m_key[i], NVM_KeySize);
        if (res == 0)//找到待删除元素
        // if (key == m_key[i]) 
        {
            //后续所有节点前移一个下标,对应指针同步操作
            for (int j = i; j < m_currentSize - 1; ++j)
            {
                SetKey(j, m_key[j+1]);
                // m_key[j] = m_key[j+1];
            }
            // m_currentSize--;
            // SetCurrentSize(m_currentSize - 1);
            m_currentSize--;

            return true;
        }
        else if (res < 0) 
        { //进入此逻辑，说明查无此KEY
            break;
        }
    }

    return false;
}

void BpNode::printKey() {
    for(int i = 0; i < m_currentSize; i++) {
        printf("%d %s;", i, m_key[i]);
    }
    printf("\n");
}

bool BpNode::DeleteAtIndex(string key) {
    bool deleted = false;
    for (int i = 0; i < m_currentSize; ++i)
    {
        DrbpStats.add_node_search();    
        int res = memcmp(key.c_str(), m_key[i], NVM_KeySize);
        if (res <= 0)
        // if (key <= m_key[i]) 
        {
            deleted = m_pointer[i]->Delete(key);
            if (m_pointer[i]->IsNeedRecoveryStructure())
            {
                //1. 先判断兄弟结点是否有多余
                if(i == 0)   // 节点只有右兄弟节点
                {
                    if(m_currentSize == 1) {
                        ;
                    }
                    else if(m_pointer[i + 1]->IsRedundant())
                    {
                        m_pointer[i]->borrowNext(m_pointer[i+1]);
                        SetKey(i, m_pointer[i]->GetMaxKey());
                    } else {
                        m_pointer[i]->mergeNext(m_pointer[i+1]);
                        SetKey(i, m_pointer[i]->GetMaxKey());
                        m_pointer[i+1]->nodeDelete();
                        deleteIndex(i + 1);
                    }
                } else {
                    if(m_pointer[i - 1]->IsRedundant())
                    {
                        m_pointer[i]->borrowPrev(m_pointer[i - 1]);
                        SetKey(i - 1, m_pointer[i - 1]->GetMaxKey());
                    } else {
                        m_pointer[i-1]->mergeNext(m_pointer[i]);
                        SetKey(i-1, m_pointer[i-1]->GetMaxKey());
                        m_pointer[i]->nodeDelete();
                        deleteIndex(i);
                    }
                }

            }
            DrbpStats.add_node_search();    
            if(m_key[i] != m_pointer[i]->GetMaxKey()) {
                SetKey(i, m_pointer[i]->GetMaxKey());
            }

            return deleted;
        }
    }

    return deleted;

}

// void BpNode::CheckLeafNode(char *key_param) {
//     if(!IsLeafNode()){
//         printf("Is not a leaf node.\n");
//     }
//     string key = string(key_param);
//     for(int i = 0; i < m_currentSize; i ++) {
//         if(key != NULL && key > m_key[i]) {
//             printf("Check Node faild.\n");
//         } 
//         key = m_key[i];
//     }
//     if(GetNext() != NULL) {
//         GetNext()->CheckLeafNode(key);
//     }
// }

// void BpNode::CheckNodeSequence(char *key_param) {
//     bool isLeaf = IsLeafNode();
//     string key = string(key_param);
//     for(int i = 0; i < m_currentSize; i ++) {
//         if(isLeaf) {
//             GetPointer(i)->CheckNodeSequence(key);
//         }
//         if(key != NULL && key > m_key[i]) {
//             printf("Check Node faild.\n");
//         } 
//         // else if(key == NULL){
//         //     printf("A null pointer check.\n");
//         // }
//         key = m_key[i];
//     }
// }

bool BpNode::Delete(string key) {
    if(IsLeafNode()) {
        return DeleteAtLeaf(key);
    } else {
        return DeleteAtIndex(key);
    }
}

void BpNode::Print(){
    bool isLeaf = IsLeafNode();
    DrBpTreeLevel_ ++;
    for(int i = 0 ; i < m_currentSize ; i++)
    {
        if(!isLeaf) {
            m_pointer[i]->Print();
        }
        uint64_t value_point;
        // memcpy(&value_point, m_key[i] + NVM_KeySize, sizeof(uint64_t));
        printf("%08d %s %llx %d\n", DrBpPrintCount_ ++, m_key[i], value_point, DrBpTreeLevel_);
    }
    DrBpTreeLevel_ --;
}

//****************************************/
//*B+树结构实现
//****************************************/
BpTree::BpTree()
{
    m_root = NULL;
    HCrchain = new RangChain;
}

BpTree::~BpTree()
{
    delete m_root;
    m_first = NULL;
    delete HCrchain;
}

void BpTree::Initialize(PersistentAllocator* valueAllocator) {
    // BpNodeNVMAllocator_ = allocator;
    DrBpValueNVMAllocator_ = valueAllocator;
}

void BpTree::InsertChain(string key)
{
    HCrchain->insert(key);
}

void BpTree::Insert(string key, string value)
{
    uint64_t vpoint;
    char keybuf[NVM_KeyBuf + 1];
    char sign[NVM_SignSize + 1];
    char *pvalue = DrBpValueNVMAllocator_->Allocate(value.size());
    vpoint = (uint64_t)pvalue;
    pmem_memcpy_persist(pvalue, value.c_str(), value.size());
    memcpy(keybuf, key.c_str(), key.size());
    memcpy(keybuf + NVM_KeySize, &vpoint, NVM_PointSize);

    snprintf(sign, sizeof(sign), "%07d", 1000000);
    string signdata(sign, NVM_SignSize);
    memcpy(keybuf + NVM_KeySize + NVM_PointSize, signdata.c_str(), NVM_SignSize);
    string tmp_key(keybuf, NVM_KeyBuf);
    // cout << "tmp_key: " << tmp_key << endl;
    // InsertChain(tmp_key);

    if(m_root == nullptr)
    {
        // assert(sizeof(BpNode) <= NVM_NodeSize);
        // printf("%s:Call\n", __FUNCTION__);
        treeLevel = 1;
        // char* mem = BpNodeNVMAllocator_->Allocate(NVM_NodeSize);
		m_root = new BpNode();
        // m_root->SetNodeType(DrBpLeafType);
        m_root->nodeType = DrBpLeafType;
        m_root->Insert(tmp_key, value);
        m_root->SetNext(nullptr);
        m_root->SetPrev(nullptr);
    }
    else
    {
        m_root->Insert(tmp_key, value);
    }
    if(m_root->IsNeedSplit())
    {
        BpNode *left = nullptr;
        BpNode *right = nullptr;
        treeLevel ++;
		m_root->Split(left, right);
        // printf("m_root split over!\n");
        // char* mem = BpNodeNVMAllocator_->Allocate(NVM_NodeSize);
        BpNode* pNewRoot = new  BpNode;
        pNewRoot->nodeType = DrBpIndexType;
        m_root = left;

        string left_key(left->GetMaxKey(),NVM_KeyBuf);
        string right_key(right->GetMaxKey(),NVM_KeyBuf);
        pNewRoot->InsertIndex(left_key, left);
        
        pNewRoot->InsertIndex(right_key, right);
        //更新根节点指针
        m_root = pNewRoot; 
    }
	//更新链表头指针
	m_first = m_root->GetMinLeaveNode();
}

void BpTree::Delete(string key)
{
    if(m_root == NULL) {
        printf("B+ tree is empty\n");
        return ;
    }
    if((m_root->Delete(key)) == true){
        if(m_root->GetSize() == 1 && !m_root->IsLeafNode()) {
            // printf("Need to Level down\n");
            treeLevel --;
            m_root = m_root->GetPointer(0);
        }
    }else 
        // printf("[DEBUG] Delet key.(%s) faild\n", key.c_str());
    return ;   
}
string BpTree::Get(const std::string& key) {
    char *pvalue;
    if(m_root == nullptr) {
        printf("B+ tree is empty\n");
        return "";
    }
    // cout << "[DEBUG] Get 1! " << endl;

    if((pvalue = m_root->Get(key)) != nullptr){
        HCrchain->update_hot(key);
        uint64_t value_point;
        memcpy(&value_point, pvalue, sizeof(uint64_t));
        char *value = (char *)value_point;
        // printf("Value pointer is %p\n", value);
        return string(value, NVM_ValueSize);
    }
    return "";
}

string BpTree::Geti(const std::string& key) {
    char *pvalue;
    if(m_root == nullptr) {
        printf("B+ tree is empty\n");
        return "";
    }
    // cout << "[DEBUG] Get 1! " << endl;

    if((pvalue = m_root->Get(key)) != nullptr){
        uint64_t value_point;
        memcpy(&value_point, pvalue, sizeof(uint64_t));
        char *value = (char *)value_point;
        // printf("Value pointer is %p\n", value);
        return string(value, NVM_ValueSize);
    }
    return "";
}

bool BpTree::Search(string key, string& value)
{
	if(m_root == NULL)
	{
		return false;
	}

	BpNode* resultNode = NULL;
	return m_root->Search(key, value, resultNode);
}

bool BpTree::Search(string key1, string key2, string* result, int& size)
{
	if(m_root == NULL || m_first == NULL)
	{
		return false;
	}

	BpNode* p = m_first;
        
	while(p != NULL)
	{
        p->Search(key1, key2, result, size);

        p = p->GetNext();
    }

    if(size == 0)
    {
        return false;
    }

    return true;
}

// void BpTree::GetRange(const std::string key1, const std::string key2, std::vector<std::string> &values, int &size) {
//     int findsize = 0;
//     BpNode* p = NULL;
//     if(m_root != NULL){
//         p = m_root->FindLeafNode(key1);
//     }
//     while(p != NULL) {
//         if(p->GetRange(key1, key2, values, findsize, size)) {
//             break;
//         }
//         p = p->GetNext();
//     }
//     size = findsize;
// }
vector<string> BpTree::OutdeData(size_t out){
    vector<string> dlist;
    cout << "size: " << HCrchain->currentSize << endl;
    // HCrchain->traver();
    for(int i = 0; i < HCrchain->theLists.size(); i++)
    {
        typename list<string>::iterator itr = HCrchain->theLists[i].begin();
        while(itr != HCrchain->theLists[i].end()){
            // cout << HCrchain->theLists[i].size() << ": " << (*itr)[(*itr).length()-8] << ": " << HCrchain->theLists[i].max_size()  << endl;
            if((*itr)[(*itr).length()-8]== '0'){
                // cout << "get !" << endl;
                dlist.push_back((*itr));
                if (dlist.size() >= out)
                    return dlist;
            }
            itr++;
        }
    }
    return dlist;
}

vector<string> BpTree::FlushtoNvm()
{
    vector<string> dlist;
    HCrchain->makeEmpty();
    BpNode* p = m_first;
    while(p!=NULL){
        for(int i=0;i<p->GetSize();i++){
            if(p->GetKey(i)[NVM_KeyBuf-8]== '1'){
                dlist.push_back(string(p->GetKey(i), NVM_KeyBuf));
                // HCrchain->update_insert(string(p->GetKey(i), NVM_KeyBuf));
                p->GetKey(i)[NVM_KeyBuf-8]= '0';
            }
            InsertChain(string(p->GetKey(i), NVM_KeyBuf));
        }
        p=p->GetNext();
    }
    return dlist;
}

// void BpTree::CreateChain()
// {
//     BpNode* p = m_root->GetMinLeaveNode();
//     HCrchain->makeEmpty();
//     HCrchain->initialize(MinHot(), MaxHot()+1);
//     while(p!=NULL){
//         HCrchain->insert(*p, p->GetHot());
//         p=p->GetNext();
//     }
//     // HCrchain->traver();
// }

void BpTree::Print(){
    if(m_root != nullptr) {
        printf("Print whole tree\n");
        // m_root->Print();
        BpNode* p = m_root->GetMinLeaveNode();
        while(p != nullptr)
	    {
            p->printKey();
            p = p->GetNext();
        }
    }
}


void BpTree::PrintStatistic() {
    DrbpStats.print_latency();
    DrbpStats.clear_period();
}

void BpTree::PrintInfo() {
    printf("This is a B+ tree\n");
    printf("NVMB_M_WAY = %d\n", DrNVMBp_M_WAY);
    printf("Tree Level = %d\n", treeLevel);
    printf("NvmNode size = %d\n", sizeof(BpNode));
    printf("B+ tree Node size = %d\n", sizeof(BpNode));
    assert(sizeof(BpNode) <= NVM_NodeSize);
}


}
