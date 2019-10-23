
#pragma once
// #include "persistent_BPlusTree.h"
#include "nvmbptree.h"
#include "persistent_skiplist_no_transaction.h"
#include <array>
using std::array;
// const size_t slots_num_bp = 1;
namespace rocksdb{
    class NVM_BPlusTree_Wrapper{
public:
    NVM_BPlusTree_Wrapper();
    ~NVM_BPlusTree_Wrapper();

    void Initialize(const std::string &path, uint64_t keysize, const std::string &valuepath, 
                uint64_t valuesize, int m=4, size_t key_size = 16, size_t buf_size = 17);
    void Insert(const string &key, const string &value);
    void Insert(const string &key, const string &value, Statistic &stats, size_t which);
 
    void Delete(const std::string& key);
    void Updakey(const std::string& key);
    string Get(const std::string& key);

    void GetRange(const std::string key1, const std::string key2, std::vector<std::string> &values, int &size);

    void FunctionTest(int ops);
    vector<string> BacktoDram(int hot, size_t read){
        return bptree_->BacktoDram(hot, read);
    }


    // bool Search(string key, string& value);
    void CreateChain(){
        bptree_->CreateChain();
    }

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
        allocator_->PrintStorage();
        valueAllocator_->PrintStorage();
    }

    bool StorageIsFull() {
        return allocator_->StorageIsFull() || valueAllocator_->StorageIsFull();
    }

    private:
        PersistentAllocator* allocator_;
        PersistentAllocator* valueAllocator_;
        NVMBpTree* bptree_;
        // array<NVMBPlusTree*, slots_num_bp> bptree_;
        size_t key_size_;
        size_t buf_size_;
    };
}
