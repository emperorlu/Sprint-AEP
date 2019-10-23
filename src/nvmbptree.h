#pragma once

#include <algorithm>
#include <math.h>
#include <string>
#include <iostream>
#include "nvm_common2.h"
#include "persistent_skiplist_no_transaction.h"
#include "libpmem.h"
#include "test_common.h"
#include "statistic.h"
#include <time.h>
#include <vector>
#include <list>
#include <cstdlib>
#include <cmath>

using namespace::std;

namespace rocksdb{
  const int NVMBp_M_WAY = (NVM_NodeSize - 2) / (NVM_PointSize + NVM_KeyBuf) - 1; 
  const int NVMBp_NODE_CAPACITY = NVMBp_M_WAY + 1;
  const int BpReserved = NVM_NodeSize - (NVMBp_NODE_CAPACITY * (NVM_PointSize + NVM_KeyBuf) + NVM_PointSize + 1);
  const int8_t BpIndexType = 0;
  const int8_t BpLeafType = 1;

class NVMBpNode    //将叶子节点和索引节点放在一个结构里面
{
  public:
    NVMBpNode()
    {
        m_currentSize = 0;
        nodeType = BpLeafType;
    }
	 ~NVMBpNode(){};

    void InsertAtLeaf(string key, string value);
    void InsertAtIndex(string key, string value);
    void Insert(string key, string value);

    ////插入键值对
    void InsertIndex(string key, NVMBpNode* _pointer);   

    //节点分裂, 输出分裂后的左右两个节点指针
    void Split(NVMBpNode* &_left, NVMBpNode* &_right);

    //查找
    bool Search(string key,  string& value, NVMBpNode*& _valueNode);
    bool Search(string key1, string key2, string* result, int& size);

	NVMBpNode* GetMinLeaveNode();

    ////删除操作

     void deleteIndex(int i) ;
     void borrowNext(NVMBpNode *nextIndex) ;
     void borrowPrev(NVMBpNode *prevIndex) ;
     void mergeNext(NVMBpNode *nextIndex) ;
     void mergePrev(NVMBpNode *prevIndex) ;
     void nodeDelete() ;
     bool DeleteAtLeaf(string key);
     bool DeleteAtIndex(string key);
     bool Delete(string key);
     void CheckLeafNode(char *);
     void CheckNodeSequence(char *);

     char *Get(const std::string& key) ;
     char *Set1(const std::string& key) ;

     void printKey();

     void Print() ;

    //删除节点后，B+树结构是否被破坏，需要恢复？
    bool IsNeedRecoveryStructure()
    {
        if (m_currentSize < (int)(ceil((NVMBp_M_WAY)/2.0)))
        {
            return true;
        }

        return false;
    }

    // void GetRangeAtIndex(const std::string& key1, const std::string& key2, std::vector<std::string> &values, int &size);

    // void GetRangeAtIndex(const std::string& key1, const std::string& key2, std::vector<std::string> &values, int &size);
    NVMBpNode *FindLeafNode(const std::string key);
    bool GetRange(const std::string key1, const std::string key2, std::vector<std::string> &values, int &findszie, int size);

    string GetValue(char *valuepointer);
    //节点是否需要分裂
    bool IsNeedSplit()
    {
        if (m_currentSize > NVMBp_M_WAY)
        {
            return true;
        }

        return false;
    }

    bool IsRedundant()
    {
        if (m_currentSize > (int)(ceil((NVMBp_M_WAY + 1) / 2.0)))
        {
            return true;
        }

        return false;
    }

    int GetHot()
    {
        int hot = 0;
        for(int i=0;i< GetSize();i++){
            hot += (m_key[i][NVM_KeyBuf-3] - '0');
        }
        return (hot / GetSize());
    }


    char* GetMaxKey()  //获取节点元素中最大KEY
    {
        if(m_currentSize == 0) return nullptr;//数据异常

        return m_key[m_currentSize-1];
    }

    void SetKey(int i, const char *key) {
        pmem_memcpy_persist(m_key[i], key, NVM_KeyBuf);
    }

    char* GetKey(int i)
    {
      return m_key[i];
    }

    void SetPointer(int i, NVMBpNode *pointer) {
        pmem_memcpy_persist(&m_pointer[i], &pointer, sizeof(NVMBpNode *));
    }

    NVMBpNode* GetPointer(int i) {
        return m_pointer[i];
    }

    void SetCurrentSize(int8_t currentsize) {
        pmem_memcpy_persist(&m_currentSize, &currentsize, sizeof(int8_t));
    }

    int8_t GetSize()
    {
        return m_currentSize;
    }

    bool IsLeafNode() {
        return nodeType == BpLeafType;
    }

    void SetNodeType(int8_t type) {
        pmem_memcpy_persist(&nodeType, &type, sizeof(int8_t));
    }

    int8_t GetNodeType() {
        return nodeType;
    }

    NVMBpNode* GetNext(){return m_pointer[0];}
    NVMBpNode* GetPrev(){return m_pointer[1];}

    void SetNext(NVMBpNode* _next) { 
        pmem_memcpy_persist(&m_pointer[0], &_next, sizeof(NVMBpNode*));
    }

    void SetPrev(NVMBpNode* _prev) {  
        pmem_memcpy_persist(&m_pointer[1], &_prev, sizeof(NVMBpNode*));
    }

  private:
    char m_key[NVMBp_NODE_CAPACITY][NVM_KeyBuf];  //[NODE_CAPACITY];
    NVMBpNode* m_pointer[NVMBp_NODE_CAPACITY];   //[NVMBp_NODE_CAPACITY]; 若为叶子节点则前两个指针分别是后继指针和前驱指针
    uint8_t m_currentSize;
    uint8_t nodeType;                            // 0:索引节点 1.叶子节点 其它：无效的数据
    uint8_t reserved[BpReserved];
};

//Chain
class NVMRangChain 
{
  public:
    NVMRangChain()
    {
        listSize = 10;
        theLists = vector<list<NVMBpNode> >(listSize);
        myList = vector<list<NVMBpNode> >(listSize);
        currentSize = 0;
    }

    void initialize(int minValue, int maxValue)
    {
        maxhot = maxValue;
        minhot = minValue;
    }

    int getSize()
    {
        return currentSize;
    }

    void makeEmpty()
    {
        for(std::size_t i = 0; i < theLists.size(); i++)
            theLists[i].clear();
    }

    void traver()
    {
        for(std::size_t i = 0; i < theLists.size(); i++)
        {
            cout << i << ":";
            typename list<NVMBpNode>::iterator itr = theLists[i].begin();
            while(itr != theLists[i].end()){
                cout << (*itr).GetHot() << "\t";
                itr++;
            }
            cout << endl;
        }
    }
    
    bool remove()
    {   
        int i = 0;
        while (theLists[i].size() == 0){
            i++;
        }
        list<NVMBpNode> & whichList = theLists[i];
        whichList.erase(whichList.begin());
        currentSize--;
        return true;
    }
    
    void relist()
    {   
        cout << "before relist: " << endl;
        // traver();
        for(std::size_t i = 0; i < theLists.size(); i++)
        {
            typename list<NVMBpNode>::iterator itr = theLists[i].begin();
            while(itr != theLists[i].end()){
                myinsert(*itr, (*itr).GetHot());
                itr++;
            }
        }
        theLists = myList;
        cout << "relist: " << endl;
        // traver();
    }
    
    bool insert(const NVMBpNode &x, int value)
    {   
        if (value < minhot){
            minhot = value;
            relist();
        }else if (value >= maxhot)
        {
            maxhot = value;
            relist();
        }
        list<NVMBpNode> & whichList = theLists[myid(value)];
        whichList.push_front(x);

        currentSize++;
        return true;   
    }
    int  maxhot, minhot;
    vector<list<NVMBpNode> > theLists;   // The array of Lists
    
  private:
    int listSize;
    int currentSize;
    vector<list<NVMBpNode>>  myList;
    // int hc; //c or h

    int myid(int value)
    {
        if(maxhot == minhot) {
            return 0;
        } 
        if(value >= maxhot) {
            return listSize - 1;
        }
        return 1.0 * (value - minhot) / (maxhot - minhot) * listSize;
    }

    bool myinsert(const NVMBpNode x, int value)
    {   
        list<NVMBpNode> & whichList = myList[myid(value)];
        whichList.push_front(x);
        return true;   
    }

};


//B+Treep定义
class NVMBpTree
{
  public:
    NVMBpTree();
    ~NVMBpTree();
    
    void Initialize(PersistentAllocator* allocator, PersistentAllocator* valueAllocator);
    void Insert(string key, string value);
    string Get(const std::string& key);
    void Updakey(const std::string& key);
    void Delete(string key);
    bool Search(string key, string& value);
    bool Search(string key1, string key2, string* result, int& size);
    void GetRange(const std::string key1, const std::string key2, std::vector<std::string> &values, int &size);
    void Print();
    void PrintStatistic();
    void PrintInfo();
    void CreateChain();
    int MinHot(){
        int minHot = m_first->GetHot();
        NVMBpNode* p = m_first;
        while(p!=NULL){
            if(p->GetHot() < minHot)
                minHot = p->GetHot();
            p=p->GetNext();
        }
        return minHot;
    }
    int MaxHot(){
        int maxHot = m_first->GetHot();
        NVMBpNode* p = m_first;
        while(p!=NULL){
            if(p->GetHot() > maxHot)
                maxHot = p->GetHot();
            p=p->GetNext();
        }
        return maxHot;
    }
    vector<string> BacktoDram(int hot, size_t read);
    NVMRangChain* HCrchain;
  private:
    //根节点指向，根节点可能是IndexNode，也可能是LeaveNode
    NVMBpNode* m_root;
    //双向链起点指向
    NVMBpNode* m_first;
    int treeLevel ;
};
}
