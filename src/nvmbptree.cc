#include "nvmbptree.h"
#include <assert.h>
#include <cmath>

namespace rocksdb{
    int  BpPrintCount_ = 0;
    int  BpTreeLevel_ = 0;
    PersistentAllocator* BpNodeNVMAllocator_;
    PersistentAllocator* BpValueNVMAllocator_;
    Statistic bpStats;

//****************************************/
//*B+树节点结构实现
//****************************************/

void NVMBpNode::InsertAtLeaf(string key, string value)
{
    assert(m_currentSize < NVMBp_NODE_CAPACITY);
    int i;
    if(m_currentSize == 0) {
        SetKey(m_currentSize, key.c_str());
        SetCurrentSize(m_currentSize + 1);
        return ;
    }
    for (i = 0; i < m_currentSize; ++i)
    {
        int res = memcmp(key.c_str(), m_key[i], NVM_KeySize);
        bpStats.add_node_search();    
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

void NVMBpNode::InsertIndex(string key, NVMBpNode *_pointer)
{
    int i;
    assert(m_currentSize < NVMBp_NODE_CAPACITY);

    if(m_currentSize == 0) {
        SetKey(m_currentSize, key.c_str());
        SetPointer(m_currentSize, _pointer);
        SetCurrentSize(m_currentSize + 1);
        return ;
    }
    for (i = 0; i < m_currentSize; ++i)
    {
        bpStats.add_node_search();
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

void NVMBpNode::InsertAtIndex(string key, string value)
{
    int res = memcmp(key.c_str(), m_key[m_currentSize-1],NVM_KeySize);
    bpStats.add_node_search();
    //考虑Key值大于现在最大值
    if(res > 0)
    {
        SetKey(m_currentSize-1, key.c_str());
        m_pointer[m_currentSize-1]->Insert(key, value);
        if(m_pointer[m_currentSize-1]->IsNeedSplit())
        {
            NVMBpNode* left = nullptr;
            NVMBpNode* right = nullptr;
            m_pointer[m_currentSize-1]->Split(left, right);

            //取left的最大key和left指针插入到当前索引节点
            string left_key(left->GetMaxKey(),NVM_KeyBuf);
            InsertIndex(left_key, left);
        }
        return ;
    }
    for (int i = 0; i < m_currentSize; ++i)
    {
        bpStats.add_node_search();
        int res = memcmp(key.c_str(), m_key[i], NVM_KeySize);
        if (res <= 0)
        {
            m_pointer[i]->Insert(key, value);
            if(m_pointer[i]->IsNeedSplit())
            {
                NVMBpNode* left = nullptr;
                NVMBpNode* right = nullptr;
                m_pointer[i]->Split(left, right);

                //取left的最大key和left指针插入到当前索引节点
                string left_key(left->GetMaxKey(),NVM_KeyBuf);
                InsertIndex(left_key, left);
            }

            break;//操作完成，即可退出
        }
    }
}

void NVMBpNode::Insert(string key, string value) {
    if(IsLeafNode()) {
        InsertAtLeaf(key, value);
    } else {
        InsertAtIndex(key, value);
    }
}

char *NVMBpNode::Get(const std::string& key) {
    for (int i = 0; i < m_currentSize; ++i)
    {
        int res = strncmp(key.c_str(), m_key[i], NVM_KeySize);
        if (res <= 0)
        {
            if(IsLeafNode()) {
                if(res == 0) {
                    if ((m_key[i][NVM_KeyBuf-3] - '0') < 79)
                        m_key[i][NVM_KeyBuf-3]++;
                    // m_key[i][NVM_KeyBuf-3]++;
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

char *NVMBpNode::Set1(const std::string& key) {
    for (int i = 0; i < m_currentSize; ++i)
    {
        int res = strncmp(key.c_str(), m_key[i], NVM_KeySize);
        if (res <= 0)
        {
            if(IsLeafNode()) {
                if(res == 0) {
                    m_key[i][NVM_KeyBuf-1] = '0';
                    return nullptr;
                }
                else {
                    return nullptr;
                }
            } else {
                return m_pointer[i]->Set1(key);
            }
        }
    }
    return nullptr;
}



NVMBpNode *NVMBpNode::FindLeafNode(const std::string key) {
    if(IsLeafNode()) {
        return this;
    }
    for (int i = 0; i < m_currentSize; ++i)
    {
        bpStats.add_node_search();
        int res = memcmp(key.c_str(), m_key[i],NVM_KeySize);
        if (res <= 0)
        {
            return m_pointer[i]->FindLeafNode(key);
        }
    }
    return nullptr;
}

void NVMBpNode::Split(NVMBpNode* &_left, NVMBpNode* &_right)
{
    assert(m_currentSize == NVMBp_NODE_CAPACITY);
    bpStats.add_split_num();
    bool isLeaf = IsLeafNode();
    char* mem = BpNodeNVMAllocator_->Allocate(NVM_NodeSize);
    NVMBpNode* pNew = new (mem) NVMBpNode;

    pNew->SetNodeType(GetNodeType());
    pNew->SetNext(nullptr);
    pNew->SetPrev(nullptr);

    int leftCount = (int)ceil((NVMBp_M_WAY+1)/2.0);
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
            SetPointer(index-leftCount, m_pointer[index]);
        }
		++index;
    }

    //更新m_currentSize值
    SetCurrentSize(rightCount);
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

NVMBpNode* NVMBpNode::GetMinLeaveNode()
{
    if(IsLeafNode()) {
	    return this; 
    } else {
        return m_pointer[0]->GetMinLeaveNode();
    }
}

bool NVMBpNode::Search(string key, string &value, NVMBpNode *&_valueNode)
{
    for (int i = 0; i < m_currentSize; ++i)
    {
        bpStats.add_node_search();    
        int res = memcmp(key.c_str(), m_key[i],NVM_KeySize);
        if (res == 0) 
        {
            // value = m_value[i];
            value = "";
            _valueNode = this;

            return true;
        }
        else if (res < 0)
        { //进入此逻辑，说明查无此KEY
            break;
        }
    }

    return false;
}


bool NVMBpNode::Search(string key1, string key2, string* result, int& size)
{
    bool bFind = false;
    for (int i = 0; i < m_currentSize; ++i)
    {
        bpStats.add_node_search();    
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

string NVMBpNode::GetValue(char *valuepointer){
    uint64_t value_point;
    memcpy(&value_point, valuepointer, sizeof(uint64_t));
    char *value = (char *)value_point;
    return string(value, NVM_ValueSize); 
}

bool NVMBpNode::GetRange(const std::string key1, const std::string key2, std::vector<std::string> &values, int &findsize, int size) {
    bool bFinished = false;
    for (int i = 0; i < m_currentSize; ++i)
    {
        bpStats.add_node_search();    
        int res1 = memcmp(key1.c_str(), m_key[i], NVM_KeySize);
        if (res1 <= 0)
        {
            if(key2.size() != 0 && memcmp(key2.c_str(), m_key[i], NVM_KeySize) < 0) {
                return true;
            }
            values.push_back(GetValue(m_key[i] + NVM_KeySize));
            findsize ++;
            if(findsize >= size) {
                return true;
            }
        }
    }
    return bFinished;
}

void NVMBpNode::deleteIndex(int i) {
    bool isLeaf = IsLeafNode();
    for(; i < m_currentSize - 1; i ++ ) {
        SetKey(i, m_key[i + 1]);
        if(!isLeaf) {
            SetPointer(i, m_pointer[i + 1]);
        }
    }
    SetCurrentSize(m_currentSize - 1);
}

void NVMBpNode::borrowNext(NVMBpNode *next) {
    bool isLeaf = IsLeafNode();
    SetKey(m_currentSize, next->GetKey(0));
    if(!isLeaf) {
        SetPointer(m_currentSize, next->GetPointer(0));
    }
    SetCurrentSize(m_currentSize + 1);
    next->deleteIndex(0);

}

void NVMBpNode::borrowPrev(NVMBpNode *prev) {
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

void NVMBpNode::mergeNext(NVMBpNode *next) {
    bool isLeaf = IsLeafNode();
    assert(m_currentSize  + next->GetSize() < NVMBp_NODE_CAPACITY);
    for(int i = 0; i < next->GetSize(); i ++) {
        SetKey(m_currentSize + i, next->GetKey(i));
        if(!isLeaf) {
            SetPointer(m_currentSize + i, next->GetPointer(i));
        }
    }
    SetCurrentSize(m_currentSize + next->GetSize());
}

void NVMBpNode::nodeDelete() {
    if(IsLeafNode()) {
        if(this->GetPrev() != nullptr)
        {
            this->GetPrev()->SetNext(this->GetNext());
        }

        if(this->GetNext() != nullptr)	 {
            this->GetNext()->SetPrev(this->GetPrev());
        }
    }
}

bool NVMBpNode::DeleteAtLeaf(string key)
{
    for (int i = 0; i < m_currentSize; ++i)
    {
        bpStats.add_node_search();    
        int res = memcmp(key.c_str(), m_key[i], NVM_KeySize);
        if (res == 0)//找到待删除元素
        {
            //后续所有节点前移一个下标,对应指针同步操作
            for (int j = i; j < m_currentSize - 1; ++j)
            {
                SetKey(j, m_key[j+1]);
            }
            // m_currentSize--;
            SetCurrentSize(m_currentSize - 1);

            return true;
        }
        else if (res < 0)
        { //进入此逻辑，说明查无此KEY
            break;
        }
    }

    return false;
}

void NVMBpNode::printKey() {
    for(int i = 0; i < m_currentSize; i++) {
        printf("%d %s;", i, m_key[i]);
    }
    printf("\n");
}

bool NVMBpNode::DeleteAtIndex(string key) {
    bool deleted = false;
    for (int i = 0; i < m_currentSize; ++i)
    {
        bpStats.add_node_search();    
        int res = memcmp(key.c_str(), m_key[i], NVM_KeySize);
        if (res <= 0)
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
            bpStats.add_node_search();    
            if(memcmp(m_key[i], m_pointer[i]->GetMaxKey(), NVM_KeySize) != 0) {
                SetKey(i, m_pointer[i]->GetMaxKey());
            }

            return deleted;
        }
    }

    return deleted;

}

void NVMBpNode::CheckLeafNode(char *key_param) {
    if(!IsLeafNode()){
        printf("Is not a leaf node.\n");
    }
    char *key = key_param;
    for(int i = 0; i < m_currentSize; i ++) {
        if(key != nullptr && memcmp(key, m_key[i], NVM_KeySize) > 0) {
            printf("Check Node faild.\n");
        } 
        key = m_key[i];
    }
    if(GetNext() != nullptr) {
        GetNext()->CheckLeafNode(key);
    }
}

void NVMBpNode::CheckNodeSequence(char *key_param) {
    bool isLeaf = IsLeafNode();
    char *key = key_param;
    for(int i = 0; i < m_currentSize; i ++) {
        if(isLeaf) {
            GetPointer(i)->CheckNodeSequence(key);
        }
        if(key != nullptr && memcmp(key, m_key[i], NVM_KeySize) > 0) {
            printf("Check Node faild.\n");
        } 
        // else if(key == nullptr){
        //     printf("A null pointer check.\n");
        // }
        key = m_key[i];
    }
}

bool NVMBpNode::Delete(string key) {
    if(IsLeafNode()) {
        return DeleteAtLeaf(key);
    } else {
        return DeleteAtIndex(key);
    }
}

void NVMBpNode::Print(){
    bool isLeaf = IsLeafNode();
    BpTreeLevel_ ++;
    for(int i = 0 ; i < m_currentSize ; i++)
    {
        if(!isLeaf) {
            m_pointer[i]->Print();
        }
        uint64_t value_point;
        memcpy(&value_point, m_key[i] + NVM_KeySize, sizeof(uint64_t));
        printf("%08d %s %llx %d\n", BpPrintCount_ ++, m_key[i], value_point, BpTreeLevel_);
    }
    BpTreeLevel_ --;
}

//****************************************/
//*B+树结构实现
//****************************************/
NVMBpTree::NVMBpTree()
{
    m_root = nullptr;
    HCrchain = new NVMRangChain;
}

NVMBpTree::~NVMBpTree()
{
    m_first = nullptr;
    HCrchain = NULL;
}

void NVMBpTree::CreateChain()
{
    NVMBpNode* p = m_first;
    HCrchain->makeEmpty();
    if(p==NULL){
        cout << "NULL!" << endl;
        return;
    }
    HCrchain->initialize(MinHot(), MaxHot()+1);
    while(p!=NULL){
        HCrchain->insert(*p, p->GetHot());
        p=p->GetNext();
    }
    // HCrchain->traver();
}


void NVMBpTree::Initialize(PersistentAllocator* allocator, PersistentAllocator* valueAllocator) {
    BpNodeNVMAllocator_ = allocator;
    BpValueNVMAllocator_ = valueAllocator;
}

void NVMBpTree::Insert(string key, string value)
{
    uint64_t vpoint;
    char keybuf[NVM_KeyBuf + 1];
    char *pvalue = BpValueNVMAllocator_->Allocate(value.size());
    vpoint = (uint64_t)pvalue;
    pmem_memcpy_persist(pvalue, value.c_str(), value.size());
    memcpy(keybuf, key.c_str(), key.size());
    memcpy(keybuf + NVM_KeySize, &vpoint, NVM_PointSize);
    
    string tmp_key(keybuf, NVM_KeyBuf);
    // cout << "key tmp_key:" << tmp_key << endl;
    // cout << "key last:" << tmp_key[NVM_KeyBuf-1] << endl;
    // bpStats.add_tree_level(treeLevel);


    if(m_root == nullptr)
    {
        assert(sizeof(NVMBpNode) <= NVM_NodeSize);
        // printf("%s:Call\n", __FUNCTION__);
        treeLevel = 1;
        char* mem = BpNodeNVMAllocator_->Allocate(NVM_NodeSize);
		m_root = new (mem) NVMBpNode();
        m_root->SetNodeType(BpLeafType);
        m_root->Insert(tmp_key, value);
        m_root->SetNext(nullptr);
        m_root->SetPrev(nullptr);
        // printf("%s:Call\n", __FUNCTION__);
    }
    else
    {
        m_root->Insert(tmp_key, value);
    }
    if(m_root->IsNeedSplit())
    {
        NVMBpNode *left = nullptr;
        NVMBpNode *right = nullptr;
        treeLevel ++;
		m_root->Split(left, right);
        // printf("m_root split over!\n");
        char* mem = BpNodeNVMAllocator_->Allocate(NVM_NodeSize);
        NVMBpNode* pNewRoot = new (mem) NVMBpNode;
        pNewRoot->SetNodeType(BpIndexType);
        m_root = left;
        string left_key(left->GetMaxKey(),NVM_KeyBuf);
        string right_key(right->GetMaxKey(),NVM_KeyBuf);
        pNewRoot->InsertIndex(left_key, left);
        m_root = pNewRoot;
        pNewRoot->InsertIndex(right_key, right);
        //更新根节点指针
        m_root = pNewRoot; 
    }
	//更新链表头指针
	m_first = m_root->GetMinLeaveNode();
}

void NVMBpTree::Delete(string key)
{
    if(m_root == nullptr) {
        printf("B+ tree is empty\n");
        bpStats.end();
        bpStats.add_delete();
        return ;
    }
    if((m_root->Delete(key)) == true){
        if(m_root->GetSize() == 1 && !m_root->IsLeafNode()) {
            // printf("Need to Level down\n");
            treeLevel --;
            m_root = m_root->GetPointer(0);
        }
    }
    // printf("Delet key.(%s) faild\n", key.c_str());
    return ;   
}



string NVMBpTree::Get(const std::string& key) {
    char *pvalue;
    if(m_root == nullptr) {
        printf("B+ tree is empty\n");
        return "";
    }

    if((pvalue = m_root->Get(key)) != nullptr){
        uint64_t value_point;
        memcpy(&value_point, pvalue, sizeof(uint64_t));
        char *value = (char *)value_point;
        // printf("Value pointer is %p\n", value);
        return string(value, NVM_ValueSize);
    }
    return "";
}

bool NVMBpTree::Search(string key, string& value)
{
	if(m_root == nullptr)
	{
		return false;
	}

	NVMBpNode* resultNode = nullptr;
	return m_root->Search(key, value, resultNode);
}

bool NVMBpTree::Search(string key1, string key2, string* result, int& size)
{
	if(m_root == nullptr || m_first == nullptr)
	{
		return false;
	}

	NVMBpNode* p = m_first;
        
	while(p != nullptr)
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

void NVMBpTree::GetRange(const std::string key1, const std::string key2, std::vector<std::string> &values, int &size) {
    int findsize = 0;
    NVMBpNode* p = nullptr;
    if(m_root != nullptr){
        p = m_root->FindLeafNode(key1);
    }
    while(p != nullptr) {
        if(p->GetRange(key1, key2, values, findsize, size)) {
            break;
        }
        p = p->GetNext();
    }
    size = findsize;
}

void NVMBpTree::Print(){
    BpPrintCount_ = 0;
    BpTreeLevel_ = 0;
    if(m_root != nullptr) {
        printf("Print whole tree\n");
        // m_root->Print();
        NVMBpNode* p = m_root->GetMinLeaveNode();
        while(p != nullptr)
	    {
            p->printKey();
            p = p->GetNext();
        }
    }
}

void NVMBpTree::PrintStatistic() {
    bpStats.print_latency();
    bpStats.clear_period();
}

void NVMBpTree::PrintInfo() {
    printf("This is a B+ tree\n");
    printf("NVMB_M_WAY = %d\n", NVMBp_M_WAY);
    printf("Tree Level = %d\n", treeLevel);
    printf("NvmNode size = %d\n", sizeof(NVMBpNode));
    printf("B+ tree Node size = %d\n", sizeof(NVMBpNode));
    assert(sizeof(NVMBpNode) <= NVM_NodeSize);
}




vector<string> NVMBpTree::BacktoDram(int hot, size_t read)
{
    vector<string> dlist;
    for(int i = HCrchain->theLists.size()-1; i >= 0; i--)
    {
        typename list<NVMBpNode>::iterator itr = HCrchain->theLists[i].begin();
        while(itr != HCrchain->theLists[i].end()){
            if((*itr).GetHot() < hot){
                return dlist;
            }
            for(int j=0;j<(*itr).GetSize();j++){
                if((*itr).GetKey(j)[NVM_KeyBuf-1]== '0'){
                    dlist.push_back(string((*itr).GetKey(j), NVM_KeyBuf));
                    (*itr).GetKey(j)[NVM_KeyBuf-1]= '1';
                    if (dlist.size() >= read)
                        return dlist;
                }
            }
            itr++;
        }
    }
    return dlist;
}

void NVMBpTree::Updakey(const std::string& key){
    if(m_root == nullptr) {
        printf("B+ tree is empty\n");
        return;
    }

    m_root->Set1(key);
}


}
