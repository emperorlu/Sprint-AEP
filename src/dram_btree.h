
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
#include "request.h"
#include "nvm_btree.h"
// #ifdef SINGLE_BTREE
// #include "single_btree.h"
// #else 
// #include "con_btree.h"
// #endif

using namespace std;

class RAMBtree{
public:
    queue<request *> req_que;
    int cache_num;
    double itime;
    double gtime;
    double nvm_gtime;
    double ftime;
    double ctime;
    double otime;
    int current_num;
    NVMBtree *bptree_nvm;
    RAMBtree();
    ~RAMBtree();

    void Initial(const std::string &valuepath, uint64_t valuesize){}
    void Initial(const std::string &path, uint64_t keysize, const std::string &valuepath, uint64_t valuesize, 
                const std::string &path2, uint64_t keysize2, const std::string &valuepath2, uint64_t valuesize2);
    void Initial(const std::string &valuepath, uint64_t valuesize, const std::string &path, 
                uint64_t keysize, const std::string &valuepath2, uint64_t valuesize2);

    void Insert(const unsigned long key, const string &value);
    void Insert(const unsigned long key, const unsigned long hot, const string &value);
    void Insert(const unsigned long key, const unsigned long hot, const string &value , int i);

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
    void FlushtoNvm(){
        gettimeofday(&be, NULL);
        lock.lock();
        vector<ram_entry> insertData = bt->range_leafs();
        lock.unlock();
        if(insertData.size()!=0){
            for(int i=0;i<insertData.size();i++){
                bptree_nvm->Insert(insertData[i].key.key, insertData[i].key.hot, string(insertData[i].ptr, NVM_ValueSize));
                current_num ++;
            }
        }
        insertData.clear();
        gettimeofday(&en, NULL);
        ftime += (en.tv_sec-be.tv_sec) + (en.tv_usec-be.tv_usec)/1000000.0;
    }

   size_t OutdeData(size_t out){
        vector<ram_entry_key_t> outData = bt->btree_out(out);
        if(outData.size()!=0){
            for(int i=0;i<outData.size();i++){
                bptree_nvm->Updakey(outData[i].key,outData[i].hot);
            }
        }
        return outData.size();
    }

    void ReadCache(size_t read){
        if (bptree_nvm->GetCacheSzie() != 0){
            cache_num++;
            vector<entry_key_t> backData = bptree_nvm->BacktoDram(MinHot(), read);
            if(backData.size()!=0){
                for(int i=0;i<backData.size();i++){
                    Insert(backData[i].key, backData[i].hot, bptree_nvm->Get(backData[i].key), 1);
                }
            }
        }
    }

    bool StorageIsFull() {
        return node_alloc->StorageIsFull() || value_alloc->StorageIsFull();
    }

    void PrintStorage() {
        node_alloc->PrintStorage();
        value_alloc->PrintStorage();
    }

    void Enque_request(request *r) {
        unique_lock<mutex> lk(lock);
        req_que.push(r);
        if(req_que.size() == 1) {
            que_cond.notify_one();
        }
    }

    void do_request(request *r) {
        switch (r->flag)
        {
            case REQ_PUT:
                gettimeofday(&nbe, NULL);
                Insert(char8toint64(r->key.c_str()), r->value);
                gettimeofday(&nen, NULL);
                itime += (nen.tv_sec-nbe.tv_sec) + (nen.tv_usec-nbe.tv_usec)/1000000.0;
                break;
            case REQ_GET:
                gettimeofday(&nbe, NULL);
                r->value = Get(char8toint64(r->key.c_str()));
                gettimeofday(&nen, NULL);
                gtime += (nen.tv_sec-nbe.tv_sec) + (nen.tv_usec-nbe.tv_usec)/1000000.0;
                break;
            case REQ_FLUSH:
                // gettimeofday(&nbe, NULL);
                // insertData = bt->range_leafs();
                // FlushtoNvm();
                // flush = 1;
                {
                    thread f(&RAMBtree::FlushtoNvm, this);
                    f.detach();
                }
                // gettimeofday(&nen, NULL);
                // ftime += (nen.tv_sec-nbe.tv_sec) + (nen.tv_usec-nbe.tv_usec)/1000000.0;
                break;
            case REQ_OUT:
                gettimeofday(&nbe, NULL);
                r->outdata = OutdeData(r->out);
                gettimeofday(&nen, NULL);
                otime += (nen.tv_sec-nbe.tv_sec) + (nen.tv_usec-nbe.tv_usec)/1000000.0;
                break;
            case REQ_CACHE:
                gettimeofday(&nbe, NULL);
                ReadCache(r->read);
                gettimeofday(&nen, NULL);
                ctime += (nen.tv_sec-nbe.tv_sec) + (nen.tv_usec-nbe.tv_usec)/1000000.0;
                break;
            case REQ_DELETE:
                Delete(char8toint64(r->key.c_str()));
                break;
            case REQ_GETC:
                gettimeofday(&nbe, NULL);
                r->value = bptree_nvm->Get(char8toint64(r->key.c_str()));
                gettimeofday(&nen, NULL);
                nvm_gtime += (nen.tv_sec-nbe.tv_sec) + (nen.tv_usec-nbe.tv_usec)/1000000.0;
                break;
            default:
                break;
        }
        unique_lock<mutex> lk(r->req_mutex);
        r->finished = true;
        r->signal.notify_one();
    }

    void worker() {
        while(!stop || !req_que.empty()) {
            // printf("Queue size is %d, stop:%d\n", req_que.size(), stop);
            while(!req_que.empty()) {
                request *r = NULL;
                lock.lock();
                r = req_que.front();
                req_que.pop();
                lock.unlock();
                if(r)
                    do_request(r);
            }
            lock.lock();
            while(req_que.empty() && !stop) {
                unique_lock<mutex> lk(lock, adopt_lock);
                que_cond.wait(lk);
                lk.release();
            }
            // if(stop){
            //     //printf("try Stop the thread..\n");
            // }
            lock.unlock();
        }
        //printf("tree %p: stopped..\n", this);
    }

private:
    NVMAllocator *value_alloc;
    ram_tree *bt;
    mutex lock;
    condition_variable que_cond;
    thread *worker_thread;
    thread *flush_thread;
    int stop,flush;
    // vector<ram_entry> insertData;
    struct timeval nbe,nen;
    struct timeval be,en;
};