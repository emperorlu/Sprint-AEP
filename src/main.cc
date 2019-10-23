#include <iostream>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <thread>

#include "nvm_common2.h"
#include "test_common.h"
#include "random.h"
#include "persistent_BPlusTree_Wrapper.h"
#include "drnvm_BPlusTree_Wrapper.h"
#include "statistic.h"
#include "rang_chain.h"
#include "dram_nvmbptree.h"


#define PATH      "/pmem0/datastruct/persistent"
#define VALUEPATH "/pmem0/datastruct/value_persistent"

#define PATH1      "/pmem1/datastruct/persistent"
#define VALUEPATH1 "/pmem1/datastruct/value_persistent" 

const size_t NVM_SIZE = 1 * (1ULL << 30);               // 45GB
const size_t NVM_VALUE_SIZE = 40960;//1 * (1ULL << 30);         // 180GB
size_t KEY_SIZE = rocksdb::NVM_KeySize;         // 32B
size_t VALUE_SIZE = rocksdb::NVM_ValueSize;         
size_t buf_size;        // initilized in parse_input()


using namespace std;

int main()
{
    rocksdb::DrNVM_BPlusTree_Wrapper *bptree_nvm0;
    rocksdb::NVM_BPlusTree_Wrapper *bptree_nvm1;
    uint64_t ops_num = 20;
    
    buf_size = KEY_SIZE + VALUE_SIZE + 1;

    bptree_nvm0 = new rocksdb::DrNVM_BPlusTree_Wrapper();
    bptree_nvm0->Initialize(VALUEPATH, NVM_VALUE_SIZE, 10, KEY_SIZE, buf_size);
    bptree_nvm0->FunctionTest(ops_num);
    bptree_nvm0->CreateChain();

    bptree_nvm1 = new rocksdb::NVM_BPlusTree_Wrapper();
    bptree_nvm1->Initialize(PATH1, NVM_SIZE, VALUEPATH1, NVM_VALUE_SIZE, 10, KEY_SIZE, buf_size);
    // bptree_nvm1->FunctionTest(ops_num);
    
    // bptree_nvm0->HCrchain.trave();

    //倒盘
    vector<string> insertData;
    insertData = bptree_nvm0->FlushtoNvm();
    cout << insertData.size() << endl;
    cout << "[Debug] flush vector" << endl;
    for(int i=0;i<insertData.size();i++){
        cout << i << endl;
        cout << "insertData: " << insertData[i] << endl;
        bptree_nvm1->Insert(insertData[i], bptree_nvm0->Get(insertData[i]));
    }
    bptree_nvm1->Print();

    //预取
    cout << "[Debug] before CreateChain" <<  endl;
    bptree_nvm1->CreateChain();
    cout << "[Debug] CreateChain " <<  endl;
    vector<string> backData;
    cout << "[Debug] before vector size: " << backData.size() << endl;
    backData = bptree_nvm1->BacktoDram(bptree_nvm0->GetMinHot());
    cout << "[Debug] vector size: " << bptree_nvm0->GetMinHot() << ":  " << backData.size() << endl;
    if(backData.size()!=0){
        for(int i=0;i<backData.size();i++){
        cout << i << endl;
        cout << "insertData: " << backData[i] << endl;
        bptree_nvm0->Insert(backData[i], bptree_nvm0->Get(backData[i]));
        }
        bptree_nvm0->Print();
    }
    
    // bptree_nvm0->HCrchain.trave();
    

    delete bptree_nvm0;
    delete bptree_nvm1;
}