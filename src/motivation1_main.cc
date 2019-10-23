#include <iostream>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <thread>

#include "test_common.h"
#include "random.h"
#include "persistent_skiplist_wrapper.h"
#include "log_batch_wrapper.h"
#include "persistent_BPlusTree_Wrapper.h"
#include "BPlusTree_Wrapper.h"
#include "persistent_BTree_Wrapper.h"

#include "persistent_hash_wrapper.h"
#include "statistic.h"
using namespace std;

// #define PATH      "/home/aep0/persistent"
// #define VALUEPATH "/home/aep0/value_persistent"
// #define PATH_LOG  "/home/aep0/dram_skiplist"

#define PATH      "/pmem0/datastruct/persistent"
#define VALUEPATH "/pmem0/datastruct/value_persistent"
#define PATH_LOG  "/pmem0/datastruct/dram_skiplist"

const size_t NVM_SIZE = 45 * (1ULL << 30);               // 45GB
const size_t NVM_VALUE_SIZE = 180 * (1ULL << 30);         // 180GB

// const size_t NVM_SIZE = 100 * (1ULL << 30);               // 45GB
// const size_t NVM_VALUE_SIZE = 500 * (1ULL << 30);         // 180GB
//const size_t NVM_LOG_SIZE = 42 * (1ULL << 30);         // 42GB
size_t KEY_SIZE = rocksdb::NVM_KeySize;         // 32B

const int HashLevelSize = 10;

// default parameter
bool using_existing_data = false;
size_t test_type = 0;
size_t VALUE_SIZE = rocksdb::NVM_ValueSize;
bool ops_type = true;
uint64_t ops_num = 1000000;
size_t mem_xxx_size = 64;       // default: 64MB
size_t xxx_max_num = 1;
size_t storage_type = 0;
size_t buf_size;        // initilized in parse_input()

rocksdb::SkiplistWriteNVM *skiplist_dram;

rocksdb::NVM_BPlusTree_Wrapper *bptree_nvm;
rocksdb::BPlusTree_Wrapper *bptree_dram;

rocksdb::NVM_BTree_Wrapper *btree_nvm;

uint64_t start_time, end_time, use_time, last_tmp_time, tmp_time, tmp_use_time;

void BTreeWriteToNvm();
void BpTreeWriteToNvm();
void SkipListWriteToNvm();
void LevelHashWriteToNvm();

void dram_print()
{
    printf("-------------   write to dram  start: ----------------------\n");
    printf("key: %uB, value: %uB, number: %llu, mem_xxx_size: %u\n", KEY_SIZE, VALUE_SIZE, ops_num, mem_xxx_size);
    printf("time: %.4f s,  speed: %.3f MB/s, IOPS: %.1f IOPS\n", 1.0*use_time*1e-6, 1.0 * (KEY_SIZE + VALUE_SIZE) * ops_num * 1e6 / use_time / 1048576, 1.0 * ops_num * 1e6 / use_time);
    printf("-------------   write to dram   end: ------------------------\n");
}

void dram_not_flush_print()
{
    uint64_t tmp1 = get_now_micros();
    uint64_t tmp2 = tmp1 - start_time;
    printf("-------------   write to dram  not flush  --- start: ----------------------\n");
    printf("key: %uB, value: %uB, number: %llu, mem_xxx_size: %u\n", KEY_SIZE, VALUE_SIZE, ops_num, mem_xxx_size);
    printf("time: %.4f s,  speed: %.3f MB/s, IOPS: %.1f IOPS\n", 1.0*tmp2*1e-6, 1.0 * (KEY_SIZE + VALUE_SIZE) * ops_num * 1e6 / tmp2 / 1048576, 1.0 * ops_num * 1e6 / tmp2);
    printf("-------------   write to dram  not flush ---  end: ------------------------\n");
}

void nvm_print(int ops_num)
{
    printf("-------------   write to nvm  start: ----------------------\n");
    printf("key: %uB, value: %uB, number: %llu\n", KEY_SIZE, VALUE_SIZE, ops_num);
    printf("time: %.4f s,  speed: %.3f MB/s, IOPS: %.1f IOPS\n", 1.0*use_time*1e-6, 1.0 * (KEY_SIZE + VALUE_SIZE) * ops_num * 1e6 / use_time / 1048576, 1.0 * ops_num * 1e6 / use_time);
    printf("-------------   write to nvm  end: ----------------------\n");
}

// 需要另外开一个线程负责模拟KV写入DRAM的skiplist结构
// 模拟数据写入到DRAM的跳表结构中
void write_to_dram()
{
    CZL_PRINT("start!");
    auto rnd = rocksdb::Random::GetTLSInstance();
    char buf[buf_size];
    memset(buf, 0, sizeof(buf));
    string value(VALUE_SIZE, 'v');
    
    start_time = get_now_micros();
    last_tmp_time = start_time;
    size_t per_1g_num = (1024 * 1000) / VALUE_SIZE * 1000;
    //string batch;
    for (uint64_t i = 1; i <= ops_num; i++) {
        auto number = rnd->Next() % ops_num;
        snprintf(buf, sizeof(buf), "%08d%08d%s", number, i, value.c_str());
        string insert_data(buf);
        if(storage_type == 0)
            skiplist_dram->Insert(insert_data);
        else if(storage_type == 2)
            bptree_dram->Insert(insert_data,"");

#ifdef EVERY_1G_PRINT
        if ((i % per_1g_num) == 0) {
            tmp_time = get_now_micros();
            tmp_use_time = tmp_time - last_tmp_time;
            printf("every 1GB (%dGB): time: %.4f s,  speed: %.3f MB/s, IOPS: %.1f IOPS\n", (i / per_1g_num), 1.0 * tmp_use_time * 1e-6, 1.0 * (KEY_SIZE + VALUE_SIZE) * per_1g_num * 1e6 / tmp_use_time / 1048576, 1.0 * per_1g_num * 1e6 / tmp_use_time);
            last_tmp_time = tmp_time;
        }
#endif
    }
    dram_not_flush_print();
    if(storage_type == 0)
        skiplist_dram->Flush();     // write DRAM data to NVM.
    else if(storage_type == 2 )
        bptree_dram->Flush();
}

void nvm_insert(int storage_type, string key, string value) {
    if(storage_type == 0) {
        skiplist_nvm->Insert(key, value);
    }else if(storage_type==1){           //btree
        btree_nvm->Insert(key, value);
    }else if(storage_type==2){          //b+tree
        bptree_nvm->Insert(key, value);
    } else if(storage_type==3){         //levle hash tree
        levelhash_nvm->Insert(key, value);
    }
}

string nvm_get(int storage_type, string key) {
    if(storage_type == 0) {
        return skiplist_nvm->Get(key);
    }else if(storage_type==1){           //btree
        return btree_nvm->Get(key);
    }else if(storage_type==2){          //b+tree
        return bptree_nvm->Get(key);
    } else if(storage_type==3){         //levle hash tree
        return levelhash_nvm->Get(key);
    }
}

void nvm_delete(int storage_type, string key) {
    if(storage_type == 0) {
        skiplist_nvm->Delete(key);
    }else if(storage_type==1){           //btree
        btree_nvm->Delete(key);
    }else if(storage_type==2){          //b+tree
        bptree_nvm->Delete(key);
    } else if(storage_type==3){         //levle hash tree
        levelhash_nvm->Delete(key);
    }
}

void nvm_scan(int storage_type, string key1, string key2, std::vector<std::string> &values, int &size) {
    if(storage_type == 0) {
        skiplist_nvm->GetRange(key1, key2, values, size);
    }else if(storage_type==1){           //btree
        btree_nvm->GetRange(key1, key2, values, size);
    }else if(storage_type==2){          //b+tree
        bptree_nvm->GetRange(key1, key2, values, size);
    } 
}

void nvm_printinfo(int storage_type) {
    if(storage_type == 0) {
        skiplist_nvm->PrintStorage();
        skiplist_nvm->PrintInfo();
    }else if(storage_type==1){           //btree
        btree_nvm->PrintStorage();
        btree_nvm->PrintInfo();
    }else if(storage_type==2){          //b+tree
        bptree_nvm->PrintStorage();
        bptree_nvm->PrintInfo();
    } else if(storage_type==3){         //levle hash tree
        levelhash_nvm->PrintStorage();
        levelhash_nvm->PrintInfo();
    }
}

void nvm_printstatics(int storage_type) {
    if(storage_type == 0) {
        skiplist_nvm->PrintStatistic();
    }else if(storage_type==1){           //btree
        btree_nvm->PrintStatistic();
    }else if(storage_type==2){          //b+tree
        bptree_nvm->PrintStatistic();
    } else if(storage_type==3){         //levle hash tree
        levelhash_nvm->PrintStatistic();
    }
}

bool isnvmfull(int storage_type) {
    if(storage_type == 0) {
        return skiplist_nvm->StorageIsFull();
    }else if(storage_type==1){           //btree
        return btree_nvm->StorageIsFull();
    }else if(storage_type==2){          //b+tree
        return bptree_nvm->StorageIsFull();
    } else if(storage_type==3){         //levle hash tree
        return levelhash_nvm->StorageIsFull();
    }
    return true;
}

const uint64_t PutOps = 200000000;
const uint64_t GetOps = 1000000;
const uint64_t DeleteOps = 1000000;
const uint64_t ScanOps = 100000;
const uint64_t ScanCount = 100;

// const uint64_t PutOps = 2000000;
// const uint64_t GetOps = 10000;
// const uint64_t DeleteOps = 10000;
// const uint64_t ScanOps = 1000;
// const uint64_t ScanCount = 100;

void motivationtest(int storage_type) {
    uint64_t i;
    char buf[KEY_SIZE];
    Statistic stats;
    string value("value", VALUE_SIZE);
    memset(buf, 0, sizeof(buf));
    printf("Key size is %d, Value size is %d.\n", KEY_SIZE, value.size());
    assert(KEY_SIZE >= 8);
    //* 随机插入测试
    rocksdb::Random rnd_insert(0xdeadbeef);
    start_time = get_now_micros();
    for (i = 1; i <= PutOps; i++) {
        uint64_t number = rnd_insert.Next();
        rocksdb::fillchar8wirhint64(buf, number);
        string data(buf, KEY_SIZE);
        stats.start();
        nvm_insert(storage_type, data, value);
        stats.end();
        stats.add_put();

        if ((i % 1000) == 0) {
            // nvm_printstatics(storage_type);
            cout<<"Put_test:"<<i;
            stats.print_latency();
            stats.clear_period();
        }

        if(isnvmfull(storage_type)) {
            break;
        }
    }
    stats.clear_period();
    end_time = get_now_micros();
    use_time = end_time - start_time;
    printf("Insert test finished\n");
    nvm_print(i-1);

    //* 随机读测试
    rocksdb::Random rnd_get(0xdeadbeef); 
    start_time = get_now_micros();
    for (i = 1; i <= GetOps; i++) {
        // auto number = rnd_get.Next() & ((1ULL << 40) - 1);
        // snprintf(buf, sizeof(buf), "%015d", number);
        uint64_t number = rnd_get.Next();
        rocksdb::fillchar8wirhint64(buf, number);
        string data(buf, KEY_SIZE);
        stats.start();
        string value = nvm_get(storage_type, data);
        stats.end();
        stats.add_get();

        if ((i % 1000) == 0) {
            cout<<"Get_test:"<<i;
            stats.print_latency();
            stats.clear_period();
        }
    }
    stats.clear_period();
    end_time = get_now_micros();
    use_time = end_time - start_time;
    printf("Get test finished\n");
    nvm_print(i-1);

    //* Scan测试
    rocksdb::Random rnd_scan(0xdeadbeef); 
    start_time = get_now_micros();
    if(storage_type != 3){         //levle hash tree
        for (i = 1; i <= ScanOps; i++) {
            // auto number = rnd_scan.Next() & ((1ULL << 40) - 1);
            // snprintf(buf, sizeof(buf), "%015d", number);
            uint64_t number = rnd_scan.Next();
            rocksdb::fillchar8wirhint64(buf, number);
            int size = ScanCount;
            string key1(buf, KEY_SIZE);
            string key2("", 0);
            std::vector<std::string> values;
            stats.start();
            nvm_scan(storage_type, key1, key2, values, size);
            stats.end();
            stats.add_scan();

            if ((i % 100) == 0) {
                cout<<"Scan_test:"<<i;
                stats.print_latency();
                stats.clear_period();
            }
        }
        stats.clear_period();
        end_time = get_now_micros();
        use_time = end_time - start_time;
        printf("Scan test finished\n");
        nvm_print(i-1);
    }

    //* 删除测试
    rocksdb::Random rnd_delete(0xdeadbeef);
    start_time = get_now_micros();
    for (i = 1; i <= DeleteOps; i++) {

        // auto number = rnd_delete.Next() & ((1ULL << 40) - 1);
        // snprintf(buf, sizeof(buf), "%015d", number);
        uint64_t number = rnd_delete.Next();
        rocksdb::fillchar8wirhint64(buf, number);
        string data(buf, KEY_SIZE);
        stats.start();
        nvm_delete(storage_type, data);
        stats.end();
        stats.add_delete();

        if ((i % 1000) == 0) {
            cout<<"Delete_test:"<<i;
            stats.print_latency();
            stats.clear_period();
        }
    }
    stats.clear_period();
    end_time = get_now_micros();
    use_time = end_time - start_time;
    printf("Delete test finished\n");
    nvm_print(i-1);

    nvm_printinfo(storage_type);
    CZL_PRINT("end!");
}

void write_to_nvm()
{
//     CZL_PRINT("start!");
//     auto rnd = rocksdb::Random::GetTLSInstance();
//     char buf[buf_size];
//     memset(buf, 0, sizeof(buf));
//     string value(VALUE_SIZE, 'v');
//     start_time = get_now_micros();
//     last_tmp_time = start_time;
//     // size_t per_1g_num = (1024 * 1024) / VALUE_SIZE * 1024 - 1;
//     size_t per_1g_num = (1024 * 16) / (buf_size) * 1024 - 1;

//     Statistic stats;
//     for (uint64_t i = 1; i <= ops_num; i++) {
//         auto number = rnd->Next() % ops_num;
//         // printf("key=%08d%08d\n",number,i);
//         snprintf(buf, sizeof(buf), "%08d%08d%s", number, i, value.c_str());
//         // snprintf(buf, sizeof(buf), "%08d%08d", number, i);
//         string data(buf,buf_size);

//         // uint64_t t1 = get_now_micros();

//         if(storage_type == 0)
//             skiplist_nvm->Insert(data);
//         else if(storage_type == 1)
//             btree_nvm->Insert(data, value,stats);
//         else if(storage_type == 2)
//             bptree_nvm->Insert(data,"",stats,0);

//         // uint64_t t2 = get_now_micros();
//         // printf("i=%d time=%.9f \n", i, 1.0*(t2-t1)*1e-6);

// #ifdef EVERY_1G_PRINT
//         if ((i % per_1g_num) == 0) {
//             tmp_time = get_now_micros();
//             tmp_use_time = tmp_time - last_tmp_time;
//             // printf("every 1GB(%dGB): time: %.4f s,  speed: %.3f MB/s, IOPS: %.1f IOPS\n", (i / per_1g_num), 1.0 * tmp_use_time * 1e-6, 1.0 * (KEY_SIZE + VALUE_SIZE) * per_1g_num * 1e6 / tmp_use_time / 1048576, 1.0 * per_1g_num * 1e6 / tmp_use_time);
//             printf("every 16MB(%d/16MB): time: %.4f s,  speed: %.3f MB/s, IOPS: %.1f IOPS\n", (i / per_1g_num), 1.0 * tmp_use_time * 1e-6, 1.0 * (KEY_SIZE + VALUE_SIZE) * per_1g_num * 1e6 / tmp_use_time / 1048576, 1.0 * per_1g_num * 1e6 / tmp_use_time);

//             // printf("every 1GB(%dGB): time: %.4f s,  speed: %.3f MB/s, IOPS: %.1f IOPS\n", (i / per_1g_num), 1.0 * tmp_use_time * 1e-6, 1.0 * (KEY_SIZE ) * per_1g_num * 1e6 / tmp_use_time / 1048576, 1.0 * per_1g_num * 1e6 / tmp_use_time);
//             last_tmp_time = tmp_time;
//             if(storage_type!=0){
//                 stats.print_latency();
//                 stats.clear_period();
//             }
//         }
// #endif
//     }
//     if(storage_type == 0){
//         skiplist_nvm->PrintLevelNum();
//         skiplist_nvm->Print();
//     }  else if(storage_type == 1) {
//         btree_nvm->PrintStorage();
//     }
//     else if(storage_type == 2) {
//         ;
//     }
//     CZL_PRINT("end!");
    if(storage_type == 0){
        SkipListWriteToNvm();
    } else if(storage_type == 1) {
        BTreeWriteToNvm();
    } else if(storage_type == 2) {
        BpTreeWriteToNvm();
    } else if(storage_type == 3){
        LevelHashWriteToNvm();
    }
}

void SkipListWriteToNvm() {
    auto rnd = rocksdb::Random::GetTLSInstance();
    char buf[buf_size];
    memset(buf, 0, sizeof(buf));
    string value(VALUE_SIZE, 'v');
    printf("Value size is %d\n", value.size());
    start_time = get_now_micros();
    last_tmp_time = start_time;
    size_t per_1g_num = (1024 * 16) / (VALUE_SIZE + KEY_SIZE) * 1024 - 1;
    skiplist_nvm->PrintInfo();
    for (uint64_t i = 1; i <= ops_num; i++) {
        auto number = rnd->Next() & ((1ULL << 40) - 1);
        // snprintf(buf, sizeof(buf), "%08d%08d%s", number, i, value.c_str());
        snprintf(buf, sizeof(buf), "%015d", number);
        string data(buf, KEY_SIZE);

        // uint64_t t1 = get_now_micros();
        skiplist_nvm->Insert(data, value);
        // uint64_t t2 = get_now_micros();

#ifdef EVERY_1G_PRINT
        if ((i % per_1g_num) == 0) {
            tmp_time = get_now_micros();
            tmp_use_time = tmp_time - last_tmp_time;
            // printf("every 1GB(%dGB): time: %.4f s,  speed: %.3f MB/s, IOPS: %.1f IOPS\n", (i / per_1g_num), 1.0 * tmp_use_time * 1e-6, 
            //          1.0 * (KEY_SIZE + VALUE_SIZE) * per_1g_num * 1e6 / tmp_use_time / 1048576, 1.0 * per_1g_num * 1e6 / tmp_use_time);
            printf("every 16MB(%d/16MB): time: %.4f s,  speed: %.3f MB/s, IOPS: %.1f IOPS\n", (i / per_1g_num), 1.0 * tmp_use_time * 1e-6,
                        1.0 * (KEY_SIZE + VALUE_SIZE) * per_1g_num * 1e6 / tmp_use_time / 1048576, 1.0 * per_1g_num * 1e6 / tmp_use_time);
            last_tmp_time = tmp_time;
            skiplist_nvm->PrintStatistic();
        }
#endif
        if(skiplist_nvm->StorageIsFull()) {
            break;
        }
    }
    // skiplist_nvm->Print();
    skiplist_nvm->PrintStorage();
    skiplist_nvm->PrintLevelNum();
    skiplist_nvm->PrintInfo();
    CZL_PRINT("end!");
}

void BTreeWriteToNvm() {
    auto rnd = rocksdb::Random::GetTLSInstance();
    char buf[buf_size];
    memset(buf, 0, sizeof(buf));
    string value(VALUE_SIZE, 'v');
    printf("Value size is %d\n", value.size());
    start_time = get_now_micros();
    last_tmp_time = start_time;
    // size_t per_1g_num = (1024 * 1024) / VALUE_SIZE * 1024 - 1;
    size_t per_1g_num = (1024 * 16) / (VALUE_SIZE + KEY_SIZE) * 1024 - 1;
    Statistic stats;
    for (uint64_t i = 1; i <= ops_num; i++) {
        auto number = rnd->Next() & ((1ULL << 40) - 1);
        // snprintf(buf, sizeof(buf), "%08d%08d%s", number, i, value.c_str());
        snprintf(buf, sizeof(buf), "%015d", number);
        string data(buf, KEY_SIZE);

        // uint64_t t1 = get_now_micros();
        btree_nvm->Insert(data, value);

        // uint64_t t2 = get_now_micros();
        // printf("i=%d time=%.9f \n", i, 1.0*(t2-t1)*1e-6);

#ifdef EVERY_1G_PRINT
        if ((i % per_1g_num) == 0) {
            tmp_time = get_now_micros();
            tmp_use_time = tmp_time - last_tmp_time;
            // printf("every 1GB(%dGB): time: %.4f s,  speed: %.3f MB/s, IOPS: %.1f IOPS\n", (i / per_1g_num), 1.0 * tmp_use_time * 1e-6, 1.0 * (KEY_SIZE + VALUE_SIZE) * per_1g_num * 1e6 / tmp_use_time / 1048576, 1.0 * per_1g_num * 1e6 / tmp_use_time);
            printf("every 16MB(%d/16MB): time: %.4f s,  speed: %.3f MB/s, IOPS: %.1f IOPS\n", (i / per_1g_num), 1.0 * tmp_use_time * 1e-6, 1.0 * (KEY_SIZE + VALUE_SIZE) * per_1g_num * 1e6 / tmp_use_time / 1048576, 1.0 * per_1g_num * 1e6 / tmp_use_time);

            // printf("every 1GB(%dGB): time: %.4f s,  speed: %.3f MB/s, IOPS: %.1f IOPS\n", (i / per_1g_num), 1.0 * tmp_use_time * 1e-6, 1.0 * (KEY_SIZE ) * per_1g_num * 1e6 / tmp_use_time / 1048576, 1.0 * per_1g_num * 1e6 / tmp_use_time);
            last_tmp_time = tmp_time;
           
            stats.print_latency();
            stats.clear_period();
        }
#endif
        if(btree_nvm->StorageIsFull()) {
            break;
        }
    }
    // btree_nvm->Print();
    btree_nvm->PrintStorage();
    btree_nvm->PrintInfo();
    CZL_PRINT("end!");
}

void BpTreeWriteToNvm() {
    auto rnd = rocksdb::Random::GetTLSInstance();
    char buf[buf_size];
    memset(buf, 0, sizeof(buf));
    string value(VALUE_SIZE, 'v');
    printf("Value size is %d\n", value.size());
    start_time = get_now_micros();
    last_tmp_time = start_time;
    // size_t per_1g_num = (1024 * 1024) / VALUE_SIZE * 1024 - 1;
    size_t per_1g_num = (1024 * 16) / (VALUE_SIZE + KEY_SIZE) * 1024 - 1;
    Statistic stats;
    bptree_nvm->PrintInfo();
    for (uint64_t i = 1; i <= ops_num; i++) {
        auto number = rnd->Next() & ((1ULL << 40) - 1);
        snprintf(buf, sizeof(buf), "%015d", number);
        string data(buf, KEY_SIZE);

        bptree_nvm->Insert(data, value);

#ifdef EVERY_1G_PRINT
        if ((i % per_1g_num) == 0) {
            tmp_time = get_now_micros();
            tmp_use_time = tmp_time - last_tmp_time;
            // printf("every 1GB(%dGB): time: %.4f s,  speed: %.3f MB/s, IOPS: %.1f IOPS\n", (i / per_1g_num), 1.0 * tmp_use_time * 1e-6, 1.0 * (KEY_SIZE + VALUE_SIZE) * per_1g_num * 1e6 / tmp_use_time / 1048576, 1.0 * per_1g_num * 1e6 / tmp_use_time);
            printf("every 16MB(%d/16MB): time: %.4f s,  speed: %.3f MB/s, IOPS: %.1f IOPS\n", (i / per_1g_num), 1.0 * tmp_use_time * 1e-6, 1.0 * (KEY_SIZE + VALUE_SIZE) * per_1g_num * 1e6 / tmp_use_time / 1048576, 1.0 * per_1g_num * 1e6 / tmp_use_time);

            // printf("every 1GB(%dGB): time: %.4f s,  speed: %.3f MB/s, IOPS: %.1f IOPS\n", (i / per_1g_num), 1.0 * tmp_use_time * 1e-6, 1.0 * (KEY_SIZE ) * per_1g_num * 1e6 / tmp_use_time / 1048576, 1.0 * per_1g_num * 1e6 / tmp_use_time);
            last_tmp_time = tmp_time;
           
            // stats.print_latency();
            stats.clear_period();
        }
#endif
        if(bptree_nvm->StorageIsFull()) {
            break;
        }
    }
    // bptree_nvm->Print();
    bptree_nvm->PrintStorage();
    bptree_nvm->PrintInfo();
    CZL_PRINT("end!");
}

void LevelHashWriteToNvm() {
    auto rnd = rocksdb::Random::GetTLSInstance();
    char buf[buf_size];
    memset(buf, 0, sizeof(buf));
    string value(VALUE_SIZE, 'v');
    printf("Value size is %d\n", value.size());
    start_time = get_now_micros();
    last_tmp_time = start_time;
    size_t per_1g_num = (1024 * 16) / (VALUE_SIZE + KEY_SIZE) * 1024 - 1;
    levelhash_nvm->PrintInfo();
    levelhash_nvm->InitStatistic();
    for (uint64_t i = 1; i <= ops_num; i++) {
        auto number = rnd->Next() & ((1ULL << 40) - 1);
        snprintf(buf, sizeof(buf), "%015d", number);
        string data(buf, KEY_SIZE);
        levelhash_nvm->Insert(data, value);

#ifdef EVERY_1G_PRINT
        if ((i % per_1g_num) == 0) {
            tmp_time = get_now_micros();
            tmp_use_time = tmp_time - last_tmp_time;
            printf("every 16MB(%d/16MB): time: %.4f s,  speed: %.3f MB/s, IOPS: %.1f IOPS\n", (i / per_1g_num), 1.0 * tmp_use_time * 1e-6,
                        1.0 * (KEY_SIZE + VALUE_SIZE) * per_1g_num * 1e6 / tmp_use_time / 1048576, 1.0 * per_1g_num * 1e6 / tmp_use_time);
            last_tmp_time = tmp_time;
            levelhash_nvm->PrintStatistic();
        }
#endif
        if(levelhash_nvm->StorageIsFull()) {
            break;
        }
    }
    // levelhash_nvm->Print();
    levelhash_nvm->PrintStorage();
    levelhash_nvm->PrintInfo();
    CZL_PRINT("end!");
}

// parse input parameter: ok return 0, else return -1;
int parse_input(int num, char **para)
{
    CZL_PRINT("num=%d", num);
    if(num == 6) {
        using_existing_data = atoi(para[1]);
        test_type = atoi(para[2]);
        ops_num = atoi(para[3]);
        VALUE_SIZE = atoi(para[4]);
        storage_type = atoi(para[5]);
        // if(test_type != 2) {
        //     return -1;
        // }
        size_t per_1g_num = (1024 * 16) / (VALUE_SIZE + KEY_SIZE) * 1024;
        printf("16Mb ops = %d\n", per_1g_num);
        return 0;
    } else if(num != 9) {
        cout << "input parameter nums incorrect! " << num << endl;
        return -1; 
    }

    using_existing_data = atoi(para[1]);
    test_type = atoi(para[2]);
    VALUE_SIZE = atoi(para[3]);
    ops_type = atoi(para[4]);
    ops_num = atoi(para[5]);
    mem_xxx_size = atoi(para[6]);
    xxx_max_num = atoi(para[7]);
    storage_type = atoi(para[8]);

    buf_size = KEY_SIZE + VALUE_SIZE + 1;

    CZL_PRINT("buf_size:%u", buf_size);
    CZL_PRINT("using_existing_data: %d(0:no, 1:yes)", using_existing_data);
    CZL_PRINT("test_type:%u(0:NVM, 1:DRAM)  value_size:%u", test_type, VALUE_SIZE);
    CZL_PRINT("ops_type:%d      ops_num:%llu", ops_type, ops_num);
    CZL_PRINT("mem_xxx_size:%uMB   xxx_max_num:%u", mem_xxx_size, xxx_max_num);
    CZL_PRINT("storage_type:%d (0:skiplist, 1:B-Tree, 2:B+ Tree, 3: Level-Hash)",storage_type);
    assert((test_type>>1) == 0);
    // assert((VALUE_SIZE & (VALUE_SIZE-1)) == 0);
//    assert(mem_xxx_size <= 4096);
    assert(xxx_max_num >= 1 && xxx_max_num <= 20);
    return 0;
}

void nvmstruct_delete(int storage_type) {
    if(storage_type == 0) {
        delete skiplist_nvm;
    }else if(storage_type==1){           //btree
        delete btree_nvm;
    }else if(storage_type==2){          //b+tree
        delete bptree_nvm;
    } else if(storage_type==3){         //levle hash tree
        delete levelhash_nvm;
    }
}

void nvm_functiontest(int storage_type, int ops_num) {
    if(storage_type == 0) {
        skiplist_nvm->FunctionTest(ops_num);
    }else if(storage_type==1){           //btree
        btree_nvm->FunctionTest(ops_num);
    }else if(storage_type==2){          //b+tree
        bptree_nvm->FunctionTest(ops_num);
    } else if(storage_type==3){         //levle hash tree
        levelhash_nvm->FunctionTest(ops_num);
    }
}

/* parameter: 
 *    @using_existing_data: whether using old data or not(read operations must rely on existing data on NVM)
 *    @test_type(0: write NVM directly, 1: write DRAM and thread flush data to NVM)
 *    @value_size
 *    @ops_type: Read=0, Write=1
 *    @ops_num: data_size = @value_size * @ops_num
 *    @mem_xxx_size: limit skiplist/bptree/btree size(MB) in DRAM. It also used to initilize DRAM skiplist
 *    @xxx_max_num: allow maximum number skiplist/bptree/btree not dealt by background thread.
 *    @storage_type: 0:skiplist, 1:B-Tree, 2:B+ Tree        // To Be Done
 */
int main(int argc, char **argv)
{
    // parse parameter
    int res = parse_input(argc, argv);
    if (res < 0) {
        cout << "parse parameter failed!" << endl;
        return 0;
    }
    // size_t per_1g_num = (1024 * 1024) / VALUE_SIZE * 1024 - 1;
    // size_t per_1g_num = (1024 * 1024) / buf_size * 1024 - 1;

    if(storage_type==0){ //skiplist
        skiplist_nvm = new rocksdb::PersistentSkiplistWrapper();
        skiplist_nvm->Initialize(PATH, NVM_SIZE, VALUEPATH, NVM_VALUE_SIZE, 15, 4);
    }else if(storage_type==1){//btree
        btree_nvm = new rocksdb::NVM_BTree_Wrapper();
        btree_nvm->Initialize(PATH, NVM_SIZE, VALUEPATH, NVM_VALUE_SIZE, 10, KEY_SIZE, buf_size);
    }else if(storage_type==2){//b+tree
        bptree_nvm = new rocksdb::NVM_BPlusTree_Wrapper();
        // bptree_nvm->Initialize(PATH, NVM_SIZE, 9, KEY_SIZE, buf_size);
        bptree_nvm->Initialize(PATH, NVM_SIZE, VALUEPATH, NVM_VALUE_SIZE, 10, KEY_SIZE, buf_size);
    } else if(storage_type==3){//levle hash tree
        levelhash_nvm = new rocksdb::PersistentLevelHashWrapper();
        // bptree_nvm->Initialize(PATH, NVM_SIZE, 9, KEY_SIZE, buf_size);
        levelhash_nvm->Initialize(PATH, NVM_SIZE, VALUEPATH, NVM_VALUE_SIZE, HashLevelSize);
    }
    
    
    CZL_PRINT("prepare to create PATH_LOG:%s", PATH_LOG);
    // KV write DRAM skiplist, and then background thread write it to NVM
    if (test_type == 1) {
        rocksdb::tpool = new ThreadPool(2);          // create threadpool(thread number = 2)
        if(storage_type==0){
            skiplist_dram = new rocksdb::SkiplistWriteNVM();
            skiplist_dram->Init(PATH_LOG, skiplist_nvm, 12, 4, mem_xxx_size * 1024 * 1024, xxx_max_num, KEY_SIZE);
        }else if(storage_type==1){

        }else if(storage_type==2){
            bptree_dram = new rocksdb::BPlusTree_Wrapper();
            bptree_dram->Initialize(PATH_LOG, bptree_nvm, 4, mem_xxx_size * 1024 * 1024, xxx_max_num, KEY_SIZE);
        }
        
        thread t(write_to_dram); // create new thread, beginning with write_to_dram()

        t.join();
        delete rocksdb::tpool;

        delete skiplist_dram;
        delete bptree_dram;
    } else if(test_type == 2){
        nvm_functiontest(storage_type, ops_num); 
    }
    else {
        motivationtest(storage_type);
    }

    // end_time = get_now_micros();
    // use_time = end_time - start_time;
    // if (test_type == 1) {
    //     dram_print();
    // } else {
    //     nvm_print();
    // }

    // delete skiplist_nvm;
    nvmstruct_delete(storage_type);
    return 0;
}
