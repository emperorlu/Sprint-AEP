#include "dram_btree.h"

RAMBtree::RAMBtree() {
    bt = new ram_tree();
    bptree_nvm= new NVMBtree();
    if(!bt) {
        assert(0);
    }
    value_alloc = nullptr;
    stop = 0;
    // flush = 0;
    itime = 0;
    gtime = 0;
    nvm_gtime = 0;
    ftime = 0;
    ctime = 0;
    otime = 0;
    cache_num = 0;
    current_num = 0;
    worker_thread = new thread(&RAMBtree::worker, this);
    // flush_thread = new thread(&RAMBtree::FlushtoNvm, this);

}

// void RAMBtree::Initial(const std::string &valuepath, uint64_t valuesize, const std::string &path, 
//                 uint64_t keysize, const std::string &valuepath2, uint64_t valuesize2) {
//     bt->btree_init();
//     value_alloc = new NVMAllocator(valuepath, valuesize);
//     if(value_alloc == nullptr) {
//         delete bt;
//         assert(0);
//     }
//     bptree_nvm->Initial(path, keysize, valuepath2, valuesize2);
// }
void RAMBtree::Initial(const std::string &path, uint64_t keysize, const std::string &valuepath, uint64_t valuesize, 
                const std::string &path2, uint64_t keysize2, const std::string &valuepath2, uint64_t valuesize2){
    bt->btree_init(path, keysize);
    value_alloc = new NVMAllocator(valuepath, valuesize);
    if(value_alloc == nullptr) {
        delete bt;
        assert(0);
    }
    bptree_nvm->Initial(path, keysize, valuepath2, valuesize2);
}

RAMBtree::~RAMBtree() {
    if(bt) {
        delete bt;
    }
    if(value_alloc) {
        delete value_alloc;
    }
    if(worker_thread) {
        stop = 1;
        que_cond.notify_one();
        //printf("Stop the thread..\n");
        worker_thread->join();
        delete worker_thread;
    }
    delete bptree_nvm;
}

// void RAMBtree::Insert(const unsigned long key, const unsigned long hot, const string &value) {
//     if(bt) {
//         char *pvalue = value_alloc->Allocate(value.size());
//         nvm_memcpy_persist(pvalue, value.c_str(), value.size(), false);
//         bt->btree_insert(ram_entry_key_t(key, 0), pvalue);
//     }
// }


    
void RAMBtree::Insert(const unsigned long key, const string &value) {
    if(bt) {
        // unique_lock<mutex> lk(lock);
        char *pvalue = value_alloc->Allocate(value.size());
        nvm_memcpy_persist(pvalue, value.c_str(), value.size(), false);

        // bt->btree_insert(key, pvalue);
        bt->btree_insert(ram_entry_key_t(key, 0, 0), pvalue);
    }
}

void RAMBtree::Insert(const unsigned long key, const unsigned long hot, const string &value){
    if(bt) {
        // unique_lock<mutex> lk(lock);
        char *pvalue = value_alloc->Allocate(value.size());
        nvm_memcpy_persist(pvalue, value.c_str(), value.size(), false);

        // bt->btree_insert(key, pvalue);
        bt->btree_insert(ram_entry_key_t(key, hot, 0), pvalue);
    }
}

void RAMBtree::Insert(const unsigned long key, const unsigned long hot, const string &value, int i){
    if(bt) {
        char *pvalue = value_alloc->Allocate(value.size());
        nvm_memcpy_persist(pvalue, value.c_str(), value.size(), false);

        bt->btree_insert(ram_entry_key_t(key, hot, 0), pvalue);
    }
}

void RAMBtree::Delete(const unsigned long  key) {
    if(bt) {
        // unique_lock<mutex> lk(lock);
        bt->btree_delete(key);
    }
}

const string RAMBtree::Get(const unsigned long key) {
    char *pvalue = NULL;
    if(bt) {
        // unique_lock<mutex> lk(lock);
        pvalue = bt->btree_search(key);
        // usleep(0);
    }
    if(pvalue) {
        return string(pvalue, NVM_ValueSize);
    }
    return "";
}

void RAMBtree::GetRange(unsigned long key1, unsigned long key2, std::vector<std::string> &values, int &size) {
    if(bt) {
        bt->btree_search_range(key1, key2, values, size);
    }
}

void RAMBtree::Print() {
    if(bt) {
        bt->printAll();
    }
}

void RAMBtree::PrintInfo() {
    if(bt) {
        bt->PrintInfo();
        show_persist_data();
    }
}

void RAMBtree::FunctionTest(int ops) {

}

void RAMBtree::motivationtest() {
    
}