#include "aep_system.h"
#include <sys/time.h>

using namespace rocksdb;

std::mutex m_mutex;
// rocksdb::NVM_BPlusTree_Wrapper *bptree_nvm0;
// rocksdb::NVM_BPlusTree_Wrapper *bptree_nvm1;
// rocksdb::NVM_BPlusTree_Wrapper *bptree_nvm2;
// rocksdb::NVM_BPlusTree_Wrapper *bptree_nvm3;
NVMBtree *bptree_nvm0;
NVMBtree *bptree_nvm1;
NVMBtree *bptree_nvm2;
NVMBtree *bptree_nvm3;

RAMBtree *dram_bptree1;
RAMBtree *dram_bptree2;
RAMBtree *dram_bptree3;

Employee e1("aep0",0);
Employee e2("aep1",1);
Employee e3("aep2",2);
Employee e4("aep3",3);
HashTable<Employee> emp_table(4);
// HashTable<Keyvalue> cache_table1(3000);
// HashTable<Keyvalue> cache_table2(3000);
// HashTable<Keyvalue> cache_table3(3000);


//大小参数
const size_t NVM_SIZE = 10 * (1ULL << 30);               // 45GB
const size_t NVM_VALUE_SIZE = 40 * (1ULL << 30);         // 180GB
const size_t CACHE_SIZE = 20 * (1ULL << 30);         // 180GB

//阈值
size_t OUT_DATA = 0;
size_t READ_DATA = 0;
size_t FLUSH_SIZE = 0; 
size_t OUT_SIZE = 0; 

// const size_t OUT_DATA = 1000000;
// const size_t READ_DATA = 500000;

const size_t OPEN_T1 = 100;
const size_t OPEN_T2 = 200;
const size_t OPEN_T3 = 300;

// const size_t FLUSH_SIZE = 600 * (1ULL << 20);
// const size_t OUT_SIZE = 6000 * (1ULL << 20);



//标记
int Dmark = 0;
int stop = 1;
// int one = 1;
int open = 1;

//统计
int current_size = 0;
int flush_size = 0;
int not_find = 0;
int dram_find = 0;
int nvm_find = 0;
int nvm1_find = 0;
int nvm2_find = 0;
int nvm3_find = 0;
int nvm0_find = 0;
int workload = 0;

int cache_num = 0;
int cache1_num = 0;
int cache2_num = 0;
int cache3_num = 0;

int flush_num = 0;
int out_num = 0;

int cache_find = 0;

int update_num1 = 0;

size_t out1_size = 0;
size_t out2_size = 0;
size_t out3_size = 0;

size_t cache1_size = 0;
size_t cache2_size = 0;
size_t cache3_size = 0;

double nvm1_itime = 0;
double nvm1_gtime = 0;
double nvm1_ctime = 0;
double nvm2_itime = 0;
double nvm2_gtime = 0;
double nvm2_ctime = 0;
double nvm3_itime = 0;
double nvm3_gtime = 0;
double nvm3_ctime = 0;
double nvm1_backtime = 0;
double nvm1_inserttime = 0;
struct timeval be1,en1;

int Find_aep(string key)
{
    if(workload > OPEN_T3)
        open = 4;
    else if(workload > OPEN_T2)
        open = 3;
    else if(workload > OPEN_T1)
        open = 2;
    emp_table.insert(e1);
    emp_table.insert(e2);
    emp_table.insert(e3);
    emp_table.insert(e4);
    return emp_table.f_key(key).getValue();
}

void OutData(int id){
    vector<ram_entry_key_t> outData;
    size_t out = OUT_DATA;
    switch (id)
    { 
        case 1:
            outData = dram_bptree1->OutdeData(out);
            out1_size += outData.size();
            if(outData.size()!=0){
                for(int i=0;i<outData.size();i++){
                    bptree_nvm1->Updakey(outData[i].key,outData[i].hot);
                    current_size--;
                }
            }
            break;
        case 2:
            outData = dram_bptree2->OutdeData(out);
            out2_size += outData.size();
            if(outData.size()!=0){
                for(int i=0;i<outData.size();i++){
                    bptree_nvm2->Updakey(outData[i].key, outData[i].hot);
                    current_size--;
                }
            }
            break;
        case 3:
            outData = dram_bptree3->OutdeData(out);
            out3_size += outData.size();
            if(outData.size()!=0){
                for(int i=0;i<outData.size();i++){
                    bptree_nvm3->Updakey(outData[i].key, outData[i].hot);
                    current_size--;
                }
            }
            break;
        default:
            cout << "error!" << endl;
    }
}

void* Data_out(void *arg) 
{
    while(stop){
        if(current_size  >= OUT_SIZE && Dmark)
        {
            out_num++;
            m_mutex.lock();
            thread ot1(OutData, 1);
            thread ot2(OutData, 2);
            thread ot3(OutData, 3);
            ot1.join();
            ot2.join();
            ot3.join();
            m_mutex.unlock();
            flush_size = current_size;
        }
    }
    pthread_exit(NULL);
}

void Read_Cache(int id)     //预取
{     
    cache_num++;
    size_t read = READ_DATA;
    vector<entry_key_t> backData;
    switch (id)
    { 
        case 1:
            if (bptree_nvm1->GetCacheSzie() != 0){
                cache1_num++;
                backData = bptree_nvm1->BacktoDram(dram_bptree1->MinHot(), read);
                cache1_size += backData.size();
                if(backData.size()!=0){
                    for(int i=0;i<backData.size();i++){
                        dram_bptree1->Insert(backData[i].key, backData[i].hot, bptree_nvm1->Get(backData[i].key));
                    }
                }
            }
            break;
        case 2:
            if (bptree_nvm2->GetCacheSzie() != 0){
                cache2_num++;
                backData = bptree_nvm2->BacktoDram(dram_bptree2->MinHot(), read);
                cache2_size += backData.size();
                if(backData.size()!=0){
                    for(int i=0;i<backData.size();i++){
                        dram_bptree2->Insert(backData[i].key, backData[i].hot, bptree_nvm2->Get(backData[i].key));
                    }
                }
            }
            break;
        case 3:
            if (bptree_nvm3->GetCacheSzie() != 0){
                cache3_num++;
                backData = bptree_nvm3->BacktoDram(dram_bptree3->MinHot(), read);
                cache3_size += backData.size();
                if(backData.size()!=0){
                    for(int i=0;i<backData.size();i++){
                        dram_bptree3->Insert(backData[i].key, backData[i].hot, bptree_nvm3->Get(backData[i].key));
                    }
                }
            }
            break;
        default:
            cout << "error!" << endl;
    }
}

void Write_Log(int id)    //倒盘
{   
    vector<ram_entry> insertData;
    switch (id)
    { 
        case 1:    
            insertData = dram_bptree1->FlushtoNvm();
            gettimeofday(&be1, NULL);
            for(int i=0;i<insertData.size();i++){
                bptree_nvm1->Insert(insertData[i].key.key, insertData[i].key.hot, string(insertData[i].ptr, NVM_ValueSize));
            }
            gettimeofday(&en1, NULL);
            nvm1_itime += (en1.tv_sec-be1.tv_sec) + (en1.tv_usec-be1.tv_usec)/1000000.0;
            break;
        case 2:
            insertData = dram_bptree2->FlushtoNvm();
            gettimeofday(&be1, NULL);
            for(int i=0;i<insertData.size();i++){
                bptree_nvm2->Insert(insertData[i].key.key, insertData[i].key.hot, string(insertData[i].ptr, NVM_ValueSize));
            }
            gettimeofday(&en1, NULL);
            nvm2_itime += (en1.tv_sec-be1.tv_sec) + (en1.tv_usec-be1.tv_usec)/1000000.0;
            break;
        case 3:
            insertData = dram_bptree3->FlushtoNvm();
            gettimeofday(&be1, NULL);
            for(int i=0;i<insertData.size();i++){
                bptree_nvm3->Insert(insertData[i].key.key, insertData[i].key.hot, string(insertData[i].ptr, NVM_ValueSize));
            }
            gettimeofday(&en1, NULL);
            nvm3_itime += (en1.tv_sec-be1.tv_sec) + (en1.tv_usec-be1.tv_usec)/1000000.0;
            break;
        default:
            cout << "error!" << endl;
    }
}  

static long insert_count = 0;
void aepsystem::Insert(const string &key, const string &value)
{
    int id = Find_aep(key);
    m_mutex.lock();
    // std::lock_guard<std::mutex> lk(m_mutex);
    insert_count++;
    // cout << "[DEBUG] Insert (" << insert_count << ") key: " << key << endl;
    if(id == 0)  // primary aep
    {
        bptree_nvm0->Insert(char8toint64(key.c_str()),value);
    }
    else        //其它aep
    {
        if ( flush_size >= FLUSH_SIZE)   //触发倒盘
        {
            Dmark = 1;
            flush_num++;
            flush_size = 0;//重新计数
            thread w1(Write_Log, 1);
            thread w2(Write_Log, 2);
            thread w3(Write_Log, 3);
            w1.join();
            w2.join();
            w3.join();
        }
        switch (id)
        { 
            case 1:
                dram_bptree1->Insert(char8toint64(key.c_str()),value);
                current_size++; 
                flush_size++;
                break;
            case 2:
                dram_bptree2->Insert(char8toint64(key.c_str()),value); 
                current_size++;  
                flush_size++;
                break;
            case 3:
                dram_bptree3->Insert(char8toint64(key.c_str()),value); 
                current_size++; 
                flush_size++;
                break;
            default:
                cout << "error!" << endl;
        }
    }
    m_mutex.unlock();
}

static long get_count = 0;
string aepsystem::Get(const std::string& key) 
{
    string tmp_value;
    int id = Find_aep(key);
    m_mutex.lock();
    // std::lock_guard<std::mutex> lk(m_mutex);
    get_count++;
    // cout << "[DEBUG] Get (" << get_count << ") key: " << char8toint64(key.c_str()) << " id: " << id << endl;
    // cout << "[DEBUG] Get (" << get_count << ") key: " << key << endl;
    if(id == 0)  // primary aep
    {
        tmp_value = bptree_nvm0->Get(char8toint64(key.c_str()));
        m_mutex.unlock();
        nvm0_find++;
        return tmp_value;
    }
    else        //其它aep
    {
        switch (id)
        {
            case 1:
                tmp_value = dram_bptree1->Get(char8toint64(key.c_str()));
                break;
            case 2:
                tmp_value = dram_bptree2->Get(char8toint64(key.c_str()));
                break;
            case 3:
                tmp_value = dram_bptree3->Get(char8toint64(key.c_str()));
                break;
            default:
                cout << "error!" << endl;
                m_mutex.unlock();
                return "";
        }
         // 没找到  在其它aep中找，并触发预取
        if(tmp_value.size() == 0) {
            if (Dmark) //至少经历一次倒盘
            {
                // cache_num++;
                if(is_cache){
                    thread r1(Read_Cache, 1);
                    thread r2(Read_Cache, 2);
                    thread r3(Read_Cache, 3);
                    r1.join();
                    r2.join();
                    r3.join();
                }
                    // Read_Cache();
            }
            switch (id)
            {
                case 1:
                    // tmp_value = bptree_nvm1->Get(char8toint64(key.c_str()));
                    gettimeofday(&be1, NULL);
                    tmp_value = bptree_nvm1->Get(char8toint64(key.c_str()));
                    gettimeofday(&en1, NULL);
                    nvm1_gtime += (en1.tv_sec-be1.tv_sec) + (en1.tv_usec-be1.tv_usec)/1000000.0;
                    nvm1_find++;
                    break;
                case 2:
                    gettimeofday(&be1, NULL);
                    tmp_value = bptree_nvm2->Get(char8toint64(key.c_str()));
                    gettimeofday(&en1, NULL);
                    nvm2_gtime += (en1.tv_sec-be1.tv_sec) + (en1.tv_usec-be1.tv_usec)/1000000.0;
                    nvm2_find++;
                    break;
                case 3:
                    gettimeofday(&be1, NULL);
                    tmp_value = bptree_nvm3->Get(char8toint64(key.c_str()));
                    gettimeofday(&en1, NULL);
                    nvm3_gtime += (en1.tv_sec-be1.tv_sec) + (en1.tv_usec-be1.tv_usec)/1000000.0;
                    nvm3_find++;
                    break;
                default:
                    cout << "error!" << endl;
                    m_mutex.unlock();
                    return "";
            }
            //没找到
            if(tmp_value.size() == 0){
                // cout << "[DEBUG] Can't find it!" << endl;
                not_find++;
                switch (id)
                {
                    case 1:
                        nvm1_find--;
                        break;
                    case 2:
                        nvm2_find--;
                        break;
                    case 3:
                        nvm3_find--;
                        break;
                    default:
                        m_mutex.unlock();
                        return "";
                   
                }
                m_mutex.unlock();
                return "";
            }
            else{
                nvm_find++;
                m_mutex.unlock();
                return tmp_value;
            } 
        }
        else{
            dram_find++;
            m_mutex.unlock();
            return tmp_value;
        }
    }
}

void aepsystem::Delete(const std::string& key)
{
    int id = Find_aep(key);
    if(id == 0)  // primary aep
    {
        // bptree_nvm0->Delete(char8toint64(key.c_str()));
        bptree_nvm0->Delete(char8toint64(key.c_str()));
    }
    else        //其它aep
    {
        switch (id)
        {
            case 1:
                dram_bptree1->Delete(char8toint64(key.c_str())); 
                current_size--; 
                flush_size--;
                break;
            case 2:
                dram_bptree2->Delete(char8toint64(key.c_str())); 
                current_size--;  
                flush_size--;
                break;
            case 3:
                dram_bptree3->Delete(char8toint64(key.c_str()));  
                current_size--; 
                flush_size--;
                break;
            default:
                cout << "error!" << endl;
        }
    }
}

aepsystem::aepsystem(){
    is_cache = 0;
    cache_size = 1;
    buf_size = KEY_SIZE + VALUE_SIZE + 1;
    // one = buf_size;
}
aepsystem::~aepsystem(){
    delete bptree_nvm0;
    delete bptree_nvm1;
    delete bptree_nvm2;
    delete bptree_nvm3;
    
    delete dram_bptree1;
    delete dram_bptree2;
    delete dram_bptree3;
}

void aepsystem::Initialize()
{
    
    OUT_SIZE = num_size * 0.8;
    FLUSH_SIZE = OUT_SIZE / 4;
    OUT_DATA = OUT_SIZE / 80;
    READ_DATA = OUT_DATA / 100;
    // READ_DATA = 1;
    cout << "System run!" << endl;
    cout << "[SIZE] FLUSH_SIZE: " << FLUSH_SIZE << endl;
    cout << "[SIZE] OUT_SIZE: " << OUT_SIZE << endl;
    cout << "[SIZE] OUT_DATA: " << OUT_DATA << endl;
    cout << "[SIZE] READ_DATA: " << READ_DATA << endl;

    bptree_nvm0= new NVMBtree();
    bptree_nvm0->Initial(PATH0, NVM_SIZE, VALUEPATH0, NVM_VALUE_SIZE);
    bptree_nvm1= new NVMBtree();
    bptree_nvm1->Initial(PATH1, NVM_SIZE, VALUEPATH1, NVM_VALUE_SIZE);
    bptree_nvm2= new NVMBtree();
    bptree_nvm2->Initial(PATH2, NVM_SIZE, VALUEPATH2, NVM_VALUE_SIZE);
    bptree_nvm3= new NVMBtree();
    bptree_nvm3->Initial(PATH3, NVM_SIZE, VALUEPATH3, NVM_VALUE_SIZE);

    dram_bptree1 = new RAMBtree();
    dram_bptree1->Initial(CACHE1, CACHE_SIZE);
    dram_bptree2 = new RAMBtree();
    dram_bptree2->Initial(CACHE2, CACHE_SIZE);
    dram_bptree3 = new RAMBtree();
    dram_bptree3->Initial(CACHE3, CACHE_SIZE);
    pthread_t t2;
    if(pthread_create(&t2, NULL, Data_out, NULL) == -1){
        puts("fail to create pthread t0");
        exit(1);
    }
    pthread_detach(t2);
    // dram_bptree1->CreateChain();

}

void aepsystem::End()
{
    stop = 0;
    cout << "[NUM] out_num: " << out_num << endl;
    cout << "[NUM] cache_num: " << cache_num << endl;
    cout << "[NUM] cache1_num: " << cache1_num << endl;
    cout << "[NUM] cache2_num: " << cache2_num << endl;
    cout << "[NUM] cache3_num: " << cache3_num << endl;
    cout << "[NUM] flush_num: " << flush_num << endl;
    cout << "[NUM] out1_size: " << out1_size << endl;
    cout << "[NUM] out2_size: " << out2_size << endl;
    cout << "[NUM] out3_size: " << out3_size << endl;
    cout << endl;
    cout << "[SIZE] current_size: " << current_size << endl;
    cout << "[SIZE] flush_size: " << flush_size << endl;
    // cout << "[SIZE] one: " << one << endl;
    cout << "[SIZE] FLUSH_SIZE: " << FLUSH_SIZE << endl;
    cout << "[SIZE] OUT_SIZE: " << OUT_SIZE << endl;
    cout << "[SIZE] OUT_DATA: " << OUT_DATA << endl;
    cout << "[SIZE] READ_DATA: " << READ_DATA << endl;

    cout << "[SIZE] cache1_size: " << cache1_size << endl;
    cout << "[SIZE] cache2_size: " << cache2_size << endl;
    cout << "[SIZE] cache3_size: " << cache3_size << endl;
    cout << endl;
    // cout << "[GET] cache_find: " << cache_find << endl;
    cout << "[GET] not_find: "  << not_find << endl;
    cout << "[GET] dram_find: "  << dram_find << endl;
    cout << "[GET] nvm_find: "  << nvm_find << endl;
    cout << "[GET] nvm1_find: "  << nvm1_find << endl;
    cout << "[GET] nvm2_find: "  << nvm2_find << endl;
    cout << "[GET] nvm3_find: "  << nvm3_find << endl;
    cout << "[GET] nvm0_find: "  << nvm0_find << endl;
    cout << endl;
    cout << "[COUNT] insert_count: "  << insert_count << endl;
    cout << "[COUNT] get_count: "  << get_count << endl;
    cout << "[COUNT] update_num1: "  << update_num1 << endl;
    cout << endl;
    cout << "[time] nvm1_itime: "  << nvm1_itime << endl;
    cout << "[time] nvm2_itime: "  << nvm2_itime << endl;
    cout << "[time] nvm3_itime: "  << nvm3_itime << endl;
    cout << "[time] nvm1_gtime: "  << nvm1_gtime << endl;
    cout << "[time] nvm2_gtime: "  << nvm2_gtime << endl;
    cout << "[time] nvm3_gtime: "  << nvm3_gtime << endl;
    cout << "[time] nvm1_ctime: "  << nvm1_ctime << endl;
    cout << "[time] nvm2_ctime: "  << nvm2_ctime << endl;
    cout << "[time] nvm3_ctime: "  << nvm3_ctime << endl;
    cout << "[time] nvm1_backtime: "  << nvm1_backtime << endl;
    cout << "[time] nvm1_inserttime: "  << nvm1_inserttime << endl;
    cout << endl;
    // cout << cache_table1.getSize() << endl;
    // cout << cache_table2.getSize() << endl;
    // cout << cache_table3.getSize() << endl;
    // dram_bptree1->CreateChain();
}

void aepsystem::Print()
{
    cout << "nvm0: " << endl;
    bptree_nvm0->Print();
    cout << "dram1: " << endl;
    dram_bptree1->Print();
    // cout << "dram2: " << endl;
    // dram_bptree2->Print();
    // cout << "dram3: " << endl;
    // dram_bptree3->Print();
    cout << "nvm1: " << endl;
    bptree_nvm1->Print();
    // cout << "nvm2: " << endl;
    // bptree_nvm2->Print();
    // cout << "nvm3: " << endl;
    // bptree_nvm3->Print();
}