#include "nvm_btree.h"

NVMBtree::NVMBtree() {
    bt = new cbtree();
    if(!bt) {
        assert(0);
    }
    value_alloc = nullptr;
    // bpnode *root = NewBpNode();
    // cbtree tmpbtree = cbtree(root);
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
}
    
void NVMBtree::Insert(const unsigned long key, const string &value) {
    if(bt) {
        char *pvalue = value_alloc->Allocate(value.size());
        nvm_memcpy_persist(pvalue, value.c_str(), value.size(), false);
        bt->btree_insert(key, pvalue);
    }
}

void NVMBtree::Delete(const unsigned long  key) {
    if(bt) {
        bt->btree_delete(key);
    }
}

const string NVMBtree::Get(const unsigned long key) {
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