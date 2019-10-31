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
  const int DrNVMBp_M_WAY = (NVM_NodeSize - 8) / (NVM_PointSize + NVM_KeyBuf) - 1; 
  const int DrNVMBp_NODE_CAPACITY = DrNVMBp_M_WAY + 1;
  const int DrBpReserved = NVM_NodeSize - (DrNVMBp_NODE_CAPACITY * (NVM_PointSize + NVM_KeyBuf) + NVM_PointSize + 1);
  const int DrBpIndexType = 0;
  const int DrBpLeafType = 1;



class BpNode    //将叶子节点和索引节点放在一个结构里面
{
  public:
    BpNode()
    {
        m_currentSize = 0;
        nodeType = DrBpLeafType;
    }


	 ~BpNode(){};

    void InsertAtLeaf(string key, string value);
    void InsertAtIndex(string key, string value);
    void Insert(string key, string value);
    void UpdateKey(string keyOld, string keyNew);

    ////插入键值对
    void InsertIndex(string key, BpNode* _pointer);   

    //节点分裂, 输出分裂后的左右两个节点指针
    void Split(BpNode* &_left, BpNode* &_right);

    //查找
    bool Search(string key,  string& value, BpNode*& _valueNode);
    bool Search(string key1, string key2, string* result, int& size);

	BpNode* GetMinLeaveNode();

    ////删除操作

     void deleteIndex(int i) ;
     void borrowNext(BpNode *nextIndex) ;
     void borrowPrev(BpNode *prevIndex) ;
     void mergeNext(BpNode *nextIndex) ;
     void mergePrev(BpNode *prevIndex) ;
     void nodeDelete() ;
     bool DeleteAtLeaf(string key);
     bool DeleteAtIndex(string key);
     bool Delete(string key);
     
     char* Get(const std::string& key) ;
     char* Geti(const std::string& key) ;

     void printKey();

     void Print() ;

    //删除节点后，B+树结构是否被破坏，需要恢复？
    bool IsNeedRecoveryStructure()
    {
        if (m_currentSize < (int)(ceil((DrNVMBp_M_WAY)/2.0)))
        {
            return true;
        }

        return false;
    }

    BpNode *FindLeafNode(const std::string key);
    bool GetRange(const std::string key1, const std::string key2, std::vector<std::string> &values, int &findszie, int size);

    string GetValue(char *valuepointer);
    //节点是否需要分裂
    bool IsNeedSplit()
    {
        if (m_currentSize > DrNVMBp_M_WAY)
        {
            return true;
        }

        return false;
    }

    bool IsRedundant()
    {
        if (m_currentSize > (int)(ceil((DrNVMBp_M_WAY + 1) / 2.0)))
        {
            return true;
        }

        return false;
    }

    char* GetMaxKey()  //获取节点元素中最大KEY
    {   
        if(m_currentSize == 0) return nullptr; //数据异常
        return m_key[m_currentSize-1];
    }

    void SetKey(int i, const char *key) {
        memcpy(m_key[i], key, NVM_KeyBuf);
    }

    char* GetKey(int i)
    {
      return m_key[i];
    }

    void SetPointer(int i, BpNode *pointer) {
        m_pointer[i] = pointer;
    }

    BpNode* GetPointer(int i) {
        return m_pointer[i];
    }

    void SetCurrentSize(int8_t currentsize) {
        m_currentSize = currentsize;
    }

    int8_t GetSize()
    {
        return m_currentSize;
    }

    bool IsLeafNode() {
        return nodeType == DrBpLeafType;
    }

    void SetNodeType(int8_t type) {
        memcpy(&nodeType, &type, sizeof(int8_t));
    }

    int8_t GetNodeType() {
        return nodeType;
    }

    BpNode* GetNext(){return m_pointer[0];}
    BpNode* GetPrev(){return m_pointer[1];}

    void SetNext(BpNode* _next) { 
        m_pointer[0] = _next;
    }

    void SetPrev(BpNode* _prev) {  
        m_pointer[1] = _prev;
    }
    int nodeType;                            // 0:索引节点 1.叶子节点 其它：无效的数据
    int m_currentSize;
  private:
    
    BpNode* m_pointer[DrNVMBp_NODE_CAPACITY];   //[DrNVMBp_NODE_CAPACITY]; 若为叶子节点则前两个指针分别是后继指针和前驱指针
    char m_key[DrNVMBp_NODE_CAPACITY][NVM_KeyBuf];
    
    uint8_t reserved[DrBpReserved];
};

//Chain
class RangChain 
{
  public:
    RangChain()
    {
        listSize = 10;
        theLists = vector<list<string> >(listSize);
        myList = vector<list<string> >(listSize);
        maxhot = 30;
        minhot = 0;
        Mah=0;
        Mih=10;
        maxSize = 100000;
        for(std::size_t i = 0; i < myList.size(); i++){
            if(!myList[i].empty()) {
                myList[i].clear();
            }
        }
        // cout << "theLists.size(): " << theLists.size() << endl;
        currentSize = 0; 
    }

    ~RangChain() {

    }

    void initialize(int minValue, int maxValue)
    {
        maxhot = maxValue;
        minhot = minValue;
    }

    int GetHot(string x)
    {
        int hot = 0;
        hot = stoi(x.substr(x.length()-7));
        return hot;
    }

    int getSize()
    {
        return currentSize;
    }

    void makeEmpty()
    {
        currentSize = 0;
        for(std::size_t i = 0; i < theLists.size(); i++){
            theLists[i].clear();
        }
        for(std::size_t i = 0; i < myList.size(); i++){
            if(!myList[i].empty()) {
                myList[i].clear();
            }
        }
    }

    void traver()
    {
        for(std::size_t i = 0; i < theLists.size(); i++)
        {
            cout << i << ":";
            typename list<string>::iterator itr = theLists[i].begin();
            while(itr != theLists[i].end()){
                cout << GetHot(*itr) << ": " << (*itr).length() << ": "; //"\t";
                cout << (*itr)[(*itr).length()-8] << "\t";
                itr++;
            }
            cout << endl;
        }
    }

    bool remove()
    {   
        cout << "remove! ";
        int i = theLists.size()-1;
        while (theLists[i].size() == 0){
            i--;
        }
        cout << i << endl;
        theLists[i].pop_front();
        currentSize--;
        return true;
    }
    
    void relist()
    {
        for(std::size_t i = 0; i < theLists.size(); i++)
        {
            typename list<string>::iterator itr = theLists[i].begin();
            while(itr != theLists[i].end()){
                myinsert(*itr, GetHot(*itr));
                itr++;
            }
        }
        theLists = myList;
    }
    
    int MinHot(){
        // int i = 0;
        // while (theLists[i].size() == 0){
        //     i++;
        // }
        // return GetHot(*theLists[i].begin());
        return Mih;
    }

    bool update_insert(const string &x)
    {
        for(std::size_t i = 0; i < theLists.size(); i++)
        {
            typename list<string>::iterator itr = theLists[i].begin();
            while(itr != theLists[i].end()){
                int res = memcmp(x.c_str(), (*itr).c_str(), NVM_KeySize);
                if (res == 0){
                    (*itr)[(*itr).length()-8] = '0';
                    return true;
                }
                itr++;
            }
        }
        return false;
    }

    bool update_hot(const string &x)
    {
        for(std::size_t i = 0; i < theLists.size(); i++)
        {
            typename list<string>::iterator itr = theLists[i].begin();
            while(itr != theLists[i].end()){
                int res = memcmp(x.c_str(), (*itr).c_str(), NVM_KeySize);
                if (res == 0){
                    theLists[i].erase(itr);
                    currentSize--;
                    insert(x);
                    return true;
                }
                itr++;
            }
        }
        return false;
    }

    bool insert(const string &x)
    {   
        // cout << "size: " << currentSize << endl;
        int value = GetHot(x);
        if(currentSize >= maxSize){
            // cout << "out! " << value << ": " << Mah << endl;
            if(value >= Mah)
                return false;
            else
                remove();
        }
        if (value > Mah){
            // cout << "Max: " << x << " value: " << value << endl;
            Mah = value;
        }
        if (value < Mih){
            Mih = value;
        }

        if (value > maxhot)
        {
            maxhot = value;
            relist();
        }else if (value < minhot){
            minhot = value;
            relist();
        }
        theLists[myid(value)].push_front(x);
        currentSize++;
        return true;   
    }
    int  maxhot, minhot;
    int currentSize;
    vector<list<string>> theLists;   // The array of Lists
    
  private:
    int listSize;
    int maxSize;
    int Mah,Mih;

    vector<list<string>>  myList;

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

    bool myinsert(const string x, int value)
    {   
        myList[myid(value)].push_front(x);
        return true;   
    }

};


//B+Treep定义
class BpTree
{
  public:
    BpTree();
    ~BpTree();
    
    void Initialize(PersistentAllocator* valueAllocator);
    void Insert(string key, string value);
    string Get(const std::string& key);
    string Geti(const std::string& key);
    void Delete(string key);
    bool Search(string key, string& value);
    bool Search(string key1, string key2, string* result, int& size);
    
    vector<string> FlushtoNvm();
    void Print();
    void PrintStatistic();
    void PrintInfo();
    // void CreateChain();
    void InsertChain(string key);
    vector<string> OutdeData(size_t out);
    int MinHot(){
        return HCrchain->MinHot();
    }
    // int GetMinHot(){
    //     return (HCrchain->minhot+HCrchain->maxhot)/2;
    // }
    // int MaxHot(){
    //     int maxHot = m_first->GetHot();
    //     BpNode* p = m_first;
    //     while(p!=NULL){
    //         if(p->GetHot() > maxHot)
    //             maxHot = p->GetHot();
    //         p=p->GetNext();
    //     }
    //     return maxHot;
    // }
    RangChain* HCrchain;
  private:
    //根节点指向，根节点可能是IndexNode，也可能是LeaveNode
    BpNode* m_root;
    //双向链起点指向
    BpNode* m_first;
    int treeLevel ;
};

}


