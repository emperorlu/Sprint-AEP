
#pragma once
#include "dram_nvmbptree.h"
#include "persistent_skiplist_no_transaction.h"
#include <array>
using std::array;
namespace rocksdb{
    class DrNVM_BPlusTree_Wrapper{
public:
    DrNVM_BPlusTree_Wrapper();
    ~DrNVM_BPlusTree_Wrapper();

    void Initialize(const std::string &valuepath, uint64_t valuesize, int m=4, size_t key_size = 16, size_t buf_size = 17);
    void Insert(const string &key, const string &value, int cache);
    void Insert(const string &key, const string &value, Statistic &stats, size_t which);

    void Delete(const std::string& key);
    vector<string> FlushtoNvm();
    vector<string> OutdeData(size_t out){
        return bptree_->OutdeData(out);
    }
    string Get(const std::string& key);
    string Geti(const std::string& key){
        return bptree_->Geti(key);
    }
    int MinHot(){
        return bptree_->MinHot();
    }
    // int GetMinHot(){
    //     return bptree_->GetMinHot();
    // }

    // void CreateChain(){
    //     bptree_->CreateChain();
    // }

    // void GetRange(const std::string key1, const std::string key2, std::vector<std::string> &values, int &size);

    void FunctionTest(int ops);
    // bool Search(string key, string& value);

    void Print() {
        bptree_->Print();
    }

    void PrintStatistic() {
        bptree_->PrintStatistic();
    }

    void PrintInfo() {
        bptree_->PrintInfo();
    }
    void PrintStorage(void) {
        // allocator_->PrintStorage();
        valueAllocator_->PrintStorage();
    }

    bool StorageIsFull() {
        return  valueAllocator_->StorageIsFull();
    }

    private:
        // PersistentAllocator* allocator_;
        PersistentAllocator* valueAllocator_;
        BpTree* bptree_;
        // array<NVMBPlusTree*, slots_num_bp> bptree_;
        size_t key_size_;
        size_t buf_size_;
    };
}
