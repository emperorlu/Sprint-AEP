#include "nvm_btree.h"

NVMBtree::NVMBtree() {
    bt = new btree();
    if(!bt) {
        assert(0);
    }
    value_alloc = nullptr;
    stop = 0;
    itime = 0;
    gtime = 0;
    // ctime = 0;
    worker_thread = new thread(&NVMBtree::worker, this);
    bpnode *root = NewBpNode();
    btree tmpbtree = btree(root);
}

void NVMBtree::Initial(const std::string &path, uint64_t keysize, const std::string &valuepath, 
                uint64_t valuesize) {
    bt->btree_init(path, keysize);
    value_alloc = new NVMAllocator(valuepath, valuesize);
    if(value_alloc == nullptr) {
        delete bt;
        assert(0);
    }
}

NVMBtree::~NVMBtree() {
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
}

void NVMBtree::Insert(const unsigned long key, const unsigned long hot, const string &value) {
    if(bt) {
        // unique_lock<mutex> lk(lock);
        // gettimeofday(&be, NULL);
        char *pvalue = value_alloc->Allocate(value.size());
        nvm_memcpy_persist(pvalue, value.c_str(), value.size(), false);
        // bt->chain_insert(entry_key_t(key, hot));
        bt->btree_insert(entry_key_t(key, hot), pvalue);
        // gettimeofday(&en, NULL);
        // itime += (en.tv_sec-be.tv_sec) + (en.tv_usec-be.tv_usec)/1000000.0;
    }
}

vector<entry_key_t> NVMBtree::BacktoDram(int hot, size_t read)
{
    if(bt) {
        unique_lock<mutex> lk(hlock);
        return bt->btree_back(hot, read);
    }
    exit(0);
}

void NVMBtree::Updakey(const unsigned long key, const unsigned long hot){
    if(bt) {
        unique_lock<mutex> lk(hlock);
        bt->btree_updakey(entry_key_t(key, hot));
    }  
}


    
void NVMBtree::Insert(const unsigned long key, const string &value) {
    if(bt) {
        // unique_lock<mutex> lk(lock);
        // gettimeofday(&be, NULL);
        char *pvalue = value_alloc->Allocate(value.size());
        nvm_memcpy_persist(pvalue, value.c_str(), value.size(), false);

        bt->btree_insert(key, pvalue);
        // gettimeofday(&en, NULL);
        // itime += (en.tv_sec-be.tv_sec) + (en.tv_usec-be.tv_usec)/1000000.0;
    }
}

void NVMBtree::Delete(const unsigned long  key) {
    if(bt) {
        // unique_lock<mutex> lk(lock);
        bt->btree_delete(key);
    }
}

const string NVMBtree::Get(const unsigned long key) {
    char *pvalue = NULL;
    if(bt) {
        // unique_lock<mutex> lk(lock);
        // gettimeofday(&be, NULL);
        pvalue = bt->btree_search(key);
        // gettimeofday(&en, NULL);
        // gtime += (en.tv_sec-be.tv_sec) + (en.tv_usec-be.tv_usec)/1000000.0;
    }
    if(pvalue) {
        // print_log(LV_DEBUG, "Get pvalue is %p.", pvalue);
        return string(pvalue, NVM_ValueSize);
    }
    return "";
}

void NVMBtree::GetRange(unsigned long key1, unsigned long key2, std::vector<std::string> &values, int &size) {
    if(bt) {
        bt->btree_search_range(key1, key2, values, size);
    }
}

void NVMBtree::Print() {
    if(bt) {
        bt->printAll();
    }
}

void NVMBtree::PrintInfo() {
    if(bt) {
        bt->PrintInfo();
        show_persist_data();
    }
}

void NVMBtree::FunctionTest(int ops) {

}

void NVMBtree::motivationtest() {
    
}