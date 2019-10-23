#include <iostream>
#include <cstdio>
#include <cstdlib>
#include <cstring>
// #include <thread>

#include "nvm_common.h"
#include "rang_chain.h"
#include "hashtable.h"
#include "bptree.h"
#include "drambptree.h"
#include "drnvm_BPlusTree_Wrapper.h"
#include "persistent_BPlusTree_Wrapper.h"
// #include "BPlusTree_Wrapper.h"
// #include "persistent_BTree_Wrapper.h"

// #include "persistent_hash_wrapper.h"
// #include "statistic.h"
using namespace std;

#define LOGSIZE  
#define CACHESIZE 

#define PATH0      "/pmem0/datastruct/persistent"
#define VALUEPATH0 "/pmem0/datastruct/value_persistent"

#define READCACHE  "/pmem0/readcache/data"

#define WRITELOG0  "/pmem0/writelog/log0"
#define WRITELOG1  "/pmem0/writelog/log1"
#define WRITELOG2  "/pmem0/writelog/log2"


#define PATH1      "/pmem1/datastruct/persistent"
#define VALUEPATH1 "/pmem1/datastruct/value_persistent"
#define PATH_LOG1  "/pmem1/datastruct/dram_skiplist"
#define PATH2      "/pmem2/datastruct/persistent"
#define VALUEPATH2 "/pmem2/datastruct/value_persistent"
#define PATH_LOG2  "/pmem2/datastruct/dram_skiplist"
#define PATH3      "/pmem3/datastruct/persistent"
#define VALUEPATH3 "/pmem3/datastruct/value_persistent"

const size_t NVM_SIZE = 45 * (1ULL << 30);               // 45GB
const size_t NVM_VALUE_SIZE = 180 * (1ULL << 30);         // 180GB
size_t KEY_SIZE = rocksdb::NVM_KeySize;         // 32B
size_t VALUE_SIZE = rocksdb::NVM_ValueSize;
size_t buf_size;        // initilized in parse_input()
int ops = 10000;
int current_size = 0;
int Dmark = 0;

rocksdb::NVM_BPlusTree_Wrapper *bptree_nvm0;
rocksdb::NVM_BPlusTree_Wrapper *bptree_nvm1;
rocksdb::NVM_BPlusTree_Wrapper *bptree_nvm2;
rocksdb::NVM_BPlusTree_Wrapper *bptree_nvm3;

rocksdb::DrNVM_BPlusTree_Wrapper *dram_bptree1;
rocksdb::DrNVM_BPlusTree_Wrapper *dram_bptree2;
rocksdb::DrNVM_BPlusTree_Wrapper *dram_bptree3;

int Find_aep(string key){
    Employee e1("aep0",0);
    Employee e2("aep1",1);
    Employee e3("aep2",2);
    Employee e4("aep3",3);

    HashTable<Employee> emp_table(4);
    emp_table.insert(e1);
    emp_table.insert(e2);
    emp_table.insert(e3);
    emp_table.insert(e4);
    return emp_table.find_key(key).getValue();
}

void Read_Cache()     //预取
{     
    //aep1
    bptree_nvm1->CreateChain();
    vector<string> backData;
    backData = bptree_nvm1->BacktoDram(dram_bptree1->GetMinHot());
    if(backData.size()!=0){
        for(int i=0;i<backData.size();i++){
            dram_bptree1->Insert(backData[i], dram_bptree1->Get(backData[i]));
        }
        dram_bptree1->Print();
    }

    //aep2
    bptree_nvm1->CreateChain();
    vector<string> backData;
    backData = bptree_nvm1->BacktoDram(dram_bptree1->GetMinHot());
    if(backData.size()!=0){
        for(int i=0;i<backData.size();i++){
            dram_bptree1->Insert(backData[i], dram_bptree1->Get(backData[i]));
        }
        dram_bptree1->Print();
    }

    //aep3
    bptree_nvm1->CreateChain();
    vector<string> backData;
    backData = bptree_nvm1->BacktoDram(dram_bptree1->GetMinHot());
    if(backData.size()!=0){
        for(int i=0;i<backData.size();i++){
            dram_bptree1->Insert(backData[i], dram_bptree1->Get(backData[i]));
        }
        dram_bptree1->Print();
    }
}

void Write_Log()    //倒盘
{   
    //aep1
    vector<string> insertData;
    insertData = dram_bptree1->FlushtoNvm();
    for(int i=0;i<insertData.size();i++){
        bptree_nvm1->Insert(insertData[i], dram_bptree1->Get(insertData[i]));
    }
    bptree_nvm1->Print();

    //aep2
    insertData = dram_bptree2->FlushtoNvm();
    for(int i=0;i<insertData.size();i++){
        bptree_nvm2->Insert(insertData[i], dram_bptree2->Get(insertData[i]));
    }
    bptree_nvm2->Print();

    //aep3
    insertData = dram_bptree3->FlushtoNvm();
    for(int i=0;i<insertData.size();i++){
        bptree_nvm3->Insert(insertData[i], dram_bptree3->Get(insertData[i]));
    }
    bptree_nvm3->Print();
}  

void Insert(const string &key, const string &value, int id)
{
    if(id == 0)  // primary aep
    {
        bptree_nvm0->Insert(key,value);
    }
    else        //其它aep
    {
        if ( current_size >= CACHESIZE * 0.8)   //触发倒盘
        {
            Dmark = 1;
            pthread_t t;
            if(pthread_create(&t, NULL, Write_Log, NULL) == -1){
                puts("fail to create pthread t0");
                exit(1);
            }
        }
        switch (id)
        {
            case 1:
                dram_bptree1->Insert(key,value);  
                break;
            case 2:
                dram_bptree2->Insert(key,value);  
                break;
            case 3:
                dram_bptree3->Insert(key,value);  
                break;
            default:
                cout << "error!" << endl;
                return 0;
        }
    }
}

string Get(const std::string& key, int id) {
    string tmp_value;
    if(id == 0)  // primary aep
    {
        tmp_value = bptree_nvm0->Get(key);
    }else        //其它aep
    {
        switch (id)
        {
            case 1:
                tmp_value = dram_bptree1->Get(key);
                break;
            case 2:
                tmp_value = dram_bptree2->Get(key);
                break;
            case 3:
                tmp_value = dram_bptree3->Get(key);
                break;
            default:
                cout << "error!" << endl;
                return nullptr;
        }
        // 没找到  在其它aep中找，并触发预取
        if(tmp_value.size() == 0) {
            if (Dmark)
            {
                pthread_t t1;
                if(pthread_create(&t1, NULL, Read_Cache, NULL) == -1){
                    puts("fail to create pthread t0");
                    exit(1);
                }
            }
            switch (id)
            {
                case 1:
                    tmp_value = bptree_nvm1->Get(key);
                    break;
                case 2:
                    tmp_value = bptree_nvm2->Get(key);
                    break;
                case 3:
                    tmp_value = bptree_nvm3->Get(key);
                    break;
                default:
                    cout << "error!" << endl;
                    return nullptr;
            }

            //没找到
            if(tmp_value.size() == 0){
                cout << "Can't find it!" << endl;
                return nullptr;
            }else{
                cout << "Find it in aeps" << endl;
                return tmp_value;
            } 
        }else{
            cout << "Find it in dram!" << endl;
            return tmp_value;
        }
    }
}

int main()
{
    buf_size = KEY_SIZE + VALUE_SIZE + 1;
    int i, id;

    //初始化
    bptree_nvm0 = new rocksdb::NVM_BPlusTree_Wrapper();
    bptree_nvm0->Initialize(PATH0, NVM_SIZE, VALUEPATH0, NVM_VALUE_SIZE, 10, KEY_SIZE, buf_size);
    bptree_nvm1 = new rocksdb::NVM_BPlusTree_Wrapper();
    bptree_nvm1->Initialize(PATH1, NVM_SIZE, VALUEPATH1, NVM_VALUE_SIZE, 10, KEY_SIZE, buf_size);
    bptree_nvm2 = new rocksdb::NVM_BPlusTree_Wrapper();
    bptree_nvm2->Initialize(PATH2, NVM_SIZE, VALUEPATH2, NVM_VALUE_SIZE, 10, KEY_SIZE, buf_size);
    bptree_nvm3 = new rocksdb::NVM_BPlusTree_Wrapper();
    bptree_nvm3->Initialize(PATH3, NVM_SIZE, VALUEPATH3, NVM_VALUE_SIZE, 10, KEY_SIZE, buf_size);
    PersistentAllocator* allocator_ = new PersistentAllocator(PATH0, NVM_SIZE);
    PersistentAllocator* valueAllocator_ = new PersistentAllocator(WRITE_LOG0, NVM_VALUE_SIZE);
    dram_bptree = new rocksdb::DRAMBpTree();
    dram_bptree->Initialize(allocator_, valueAllocator_);



    char keybuf[KEY_SIZE + 1];
    char valuebuf[VALUE_SIZE + 1];
    PrintInfo();
    printf("******Test Start.******\n");
    for(i = 0; i < ops; i ++) {
        snprintf(keybuf, sizeof(keybuf), "%07d", i);
        snprintf(valuebuf, sizeof(valuebuf), "%020d", i * i);
        string data(keybuf, KEY_SIZE);
        string value(valuebuf, VALUE_SIZE);
        id = Find_aep(data);
        Insert(data, value, id);
    }
    printf("******Insert test finished.******\n");

    for(i = 0; i < ops; i ++) 
    {
        snprintf(keybuf, sizeof(keybuf), "%07d", i);
        snprintf(valuebuf, sizeof(valuebuf), "%020d", i * i);
        string data(keybuf, KEY_SIZE);
        string value(valuebuf, VALUE_SIZE);
        id = Find_aep(data);
        string tmp_value = Get(data, id);
        if(tmp_value.size() == 0) {
            printf("Error: Get key-value faild.(key:%s)\n", data.c_str());
        } else if(strncmp(value.c_str(), tmp_value.c_str(), VALUE_SIZE) != 0) {
            printf("Error: Get key-value faild.(Expect:%s, but Get %s)\n", value.c_str(), tmp_value.c_str());
        }
    }
    printf("******Get test finished.*****\n");
}


