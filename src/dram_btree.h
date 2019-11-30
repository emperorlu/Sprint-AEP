
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
// #include "request.h"
// #ifdef SINGLE_BTREE
// #include "single_btree.h"
// #else 
// #include "con_btree.h"
// #endif

using namespace std;

class RAMBtree{
public:
    // queue<request *> req_que;
    // double itime;
    // double gtime;
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
        unique_lock<mutex> lk(lock);
        return bt->range_leafs();
    }

    vector<ram_entry_key_t> OutdeData(size_t out){
        unique_lock<mutex> lk(lock);
        return bt->btree_out(out);
    }

    bool StorageIsFull() {
        return node_alloc->StorageIsFull() || value_alloc->StorageIsFull();
    }

    void PrintStorage() {
        node_alloc->PrintStorage();
        value_alloc->PrintStorage();
    }

    //     void Enque_request(request *r) {
    //     unique_lock<mutex> lk(lock);
    //     req_que.push(r);
    //     if(req_que.size() == 1) {
    //         que_cond.notify_one();
    //     }
    // }

    // void do_request(request *r) {
    //     switch (r->flag)
    //     {
    //         case REQ_PUT:
    //             gettimeofday(&nbe, NULL);
    //             Insert(char8toint64(r->key.c_str()), r->value);
    //             gettimeofday(&nen, NULL);
    //             itime += (nen.tv_sec-nbe.tv_sec) + (nen.tv_usec-nbe.tv_usec)/1000000.0;
    //             break;
    //         case REQ_GET:
    //             gettimeofday(&nbe, NULL);
    //             r->value = Get(char8toint64(r->key.c_str()));
    //             gettimeofday(&nen, NULL);
    //             gtime += (nen.tv_sec-nbe.tv_sec) + (nen.tv_usec-nbe.tv_usec)/1000000.0;
    //             break;
    //         case REQ_FLUSH:
    //             r->flushData = FlushtoNvm();
    //             break;
    //         case REQ_DELETE:
    //             Delete(char8toint64(r->key.c_str()));
    //             break;
    //         default:
    //             break;
    //     }
    //     unique_lock<mutex> lk(r->req_mutex);
    //     r->finished = true;
    //     r->signal.notify_one();
    // }

    // void worker() {
    //     while(!stop || !req_que.empty()) {
    //         // printf("Queue size is %d, stop:%d\n", req_que.size(), stop);
    //         while(!req_que.empty()) {
    //             request *r = NULL;
    //             lock.lock();
    //             r = req_que.front();
    //             req_que.pop();
    //             lock.unlock();
    //             if(r)
    //                 do_request(r);
    //         }
    //         lock.lock();
    //         while(req_que.empty() && !stop) {
    //             unique_lock<mutex> lk(lock, adopt_lock);
    //             que_cond.wait(lk);
    //             lk.release();
    //         }
    //         // if(stop){
    //         //     //printf("try Stop the thread..\n");
    //         // }
    //         lock.unlock();
    //     }
    //     //printf("tree %p: stopped..\n", this);
    // }

private:
    NVMAllocator *value_alloc;
    ram_tree *bt;
    mutex lock;
    // condition_variable que_cond;
    // thread *worker_thread;
    // int stop;
    // struct timeval nbe,nen;
};