#include "dram_btree.h"

RAMBtree::RAMBtree() {
    bt = new ram_tree();
    if(!bt) {
        assert(0);
    }
    value_alloc = nullptr;
    // bpnode *root = NewBpNode();
    // ram_tree tmpbtree = ram_tree(root);
}

void RAMBtree::Initial(const std::string &valuepath, uint64_t valuesize) {
    bt->btree_init();
    value_alloc = new NVMAllocator(valuepath, valuesize);
    if(value_alloc == nullptr) {
        delete bt;
        assert(0);
    }
}

RAMBtree::~RAMBtree() {
    if(bt) {
        delete bt;
    }
    if(value_alloc) {
        delete value_alloc;
    }
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
        char *pvalue = value_alloc->Allocate(value.size());
        nvm_memcpy_persist(pvalue, value.c_str(), value.size(), false);

        // bt->btree_insert(key, pvalue);
        bt->btree_insert(ram_entry_key_t(key, 0, 0), pvalue);
    }
}

void RAMBtree::Insert(const unsigned long key, const unsigned long hot, const string &value){
    if(bt) {
        char *pvalue = value_alloc->Allocate(value.size());
        nvm_memcpy_persist(pvalue, value.c_str(), value.size(), false);

        // bt->btree_insert(key, pvalue);
        bt->btree_insert(ram_entry_key_t(key, hot, 0), pvalue);
    }
}

void RAMBtree::Delete(const unsigned long  key) {
    if(bt) {
        bt->btree_delete(key);
    }
}

const string RAMBtree::Get(const unsigned long key) {
    char *pvalue = NULL;
    if(bt) {
        pvalue = bt->btree_search(key);
    }
    if(pvalue) {
        // print_log(LV_DEBUG, "Get pvalue is %p.", pvalue);
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