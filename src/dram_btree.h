
#pragma once

/*
* nvm_btree.h
*/

#include <array>
#include <thread>
#include <mutex>
#include <string>
#include <assert.h>

#include "nvm_common2.h"
#include "nvm_allocator.h"
#include "ram_btree.h"
// #ifdef SINGLE_BTREE
// #include "single_btree.h"
// #else 
// #include "con_btree.h"
// #endif

using namespace std;

class RAMBtree{
public:
    RAMBtree();
    ~RAMBtree();

    void Initial(const std::string &valuepath, uint64_t valuesize);

    void Insert(const unsigned long key, const string &value);
    void Insert(const unsigned long key, const unsigned long hot, const string &value);

    void Delete(const unsigned long key);

    const std::string Get(const unsigned long key);

    void GetRange(unsigned long key1, unsigned long key2, std::vector<std::string> &values, int &size);

    void FunctionTest(int ops);
    void motivationtest();
    void Print();
    void PrintInfo();
    int MinHot(){
        return bt->minHot();
    }
    vector<ram_entry> FlushtoNvm(){
        return bt->range_leafs();
    }

    vector<ram_entry_key_t> OutdeData(size_t out){
        return bt->btree_out(out);
    }

    bool StorageIsFull() {
        return node_alloc->StorageIsFull() || value_alloc->StorageIsFull();
    }

    void PrintStorage() {
        node_alloc->PrintStorage();
        value_alloc->PrintStorage();
    }

private:
    NVMAllocator *value_alloc;
    ram_tree *bt;
};