#include "aep_system.h"
#include <sys/time.h>

using namespace rocksdb;

NVMBtree *bptree_nvm0;
NVMBtree *bptree_nvm1;
NVMBtree *bptree_nvm2;
RAMBtree *dram_bptree3;

Employee e1("aep0",0);
Employee e2("aep1",1);
Employee e3("aep2",2);
Employee e4("aep3",3);
HashTable<Employee> emp_table(4);


//大小参数
const size_t NVM_SIZE = 10 * (1ULL << 30);               // 45GB
const size_t NVM_VALUE_SIZE = 40 * (1ULL << 30);         // 180GB
const size_t CACHE_SIZE = 20 * (1ULL << 30);         // 180GB

//阈值
size_t OUT_DATA = 0;
size_t READ_DATA = 0;
size_t FLUSH_SIZE = 0; 
size_t OUT_SIZE = 0; 


//标记
int Dmark = 0;
int stop = 1;

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

int cache_num = 0;
int cache1_num = 0;
int cache2_num = 0;
int cache3_num = 0;

int flush_num = 0;
int out_num = 0;
double nvm1_itime = 0;
double nvm1_gtime = 0;
double nvm1_ctime = 0;
double nvm2_itime = 0;
double nvm2_gtime = 0;
double nvm2_ctime = 0;
double nvm3_itime = 0;
double nvm3_gtime = 0;
double nvm3_ctime = 0;

struct timeval be1,en1;

int Find_aep(string key)
{
    emp_table.insert(e1);
    emp_table.insert(e2);
    emp_table.insert(e3);
    emp_table.insert(e4);
    return emp_table.f_key(key).getValue();
}

void DataOut(int id){
    request req;
    req.out = OUT_DATA;
    req.flag = REQ_OUT;
    req.finished = false;
    vector<ram_entry_key_t> outData;
    size_t out = OUT_DATA;
#ifdef USE_MUIL_THREAD
            {
                dram_bptree3->Enque_request(&req);
                unique_lock<mutex> lk(req.req_mutex);
                while(!req.finished) {
                    req.signal.wait(lk);
                }
            }
            current_size -= req.outdata;
#else
            outData = dram_bptree3->OutdeData(out);
            if(outData.size()!=0){
                for(int i=0;i<outData.size();i++){
                    dram_bptree3->bptree_nvm->Updakey(outData[i].key,outData[i].hot);
                    current_size--;
                }
            }
#endif
}

void* Data_out(void *arg) 
{
    while(stop){
        if(current_size  >= OUT_SIZE && Dmark)
        {
            out_num++;
            thread o3(DataOut, 3);
            o3.join();
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
    request req;
    req.read = READ_DATA;
    req.flag = REQ_CACHE;
    req.finished = false;
#ifdef USE_MUIL_THREAD
            {
                dram_bptree3->Enque_request(&req);
                unique_lock<mutex> lk(req.req_mutex);
                while(!req.finished) {
                    req.signal.wait(lk);
                }
            }
#else
            if (dram_bptree3->bptree_nvm->GetCacheSzie() != 0){
                cache3_num++;
                backData = dram_bptree3->bptree_nvm->BacktoDram(dram_bptree3->MinHot(), read);
                if(backData.size()!=0){
                    for(int i=0;i<backData.size();i++){
                        dram_bptree3->Insert(backData[i].key, backData[i].hot, dram_bptree3->bptree_nvm->Get(backData[i].key));
                    }
                }
            }
#endif
}

void Write_Log()    //倒盘
{   
    vector<ram_entry> insertData;
    request req;
    req.flag = REQ_FLUSH;
    req.finished = false;
#ifdef USE_MUIL_THREAD
            {
                dram_bptree3->Enque_request(&req);
                unique_lock<mutex> lk(req.req_mutex);
                while(!req.finished) {
                    req.signal.wait(lk);
                }
            }
#else
            insertData = dram_bptree3->FlushtoNvm();
            gettimeofday(&be1, NULL);
            for(int i=0;i<insertData.size();i++){
                dram_bptree3->bptree_nvm->Insert(insertData[i].key.key, insertData[i].key.hot, string(insertData[i].ptr, NVM_ValueSize));
            }
            gettimeofday(&en1, NULL);
            nvm3_itime += (en1.tv_sec-be1.tv_sec) + (en1.tv_usec-be1.tv_usec)/1000000.0;
#endif
}  

static long insert_count = 0;
void aepsystem::Insert(const string &key, const string &value)
{
    int id = Find_aep(key);
    insert_count++;
    request req;
    req.key = key;
    req.value = value;
    req.flag = REQ_PUT;
    req.finished = false;
    if(id == 0)  // primary aep
    {
#ifdef USE_MUIL_THREAD
        {
            bptree_nvm0->Enque_request(&req);
            unique_lock<mutex> lk(req.req_mutex);
            while(!req.finished) {
                req.signal.wait(lk);
            }
        }
#else
        bptree_nvm0->Insert(char8toint64(key.c_str()), value);
#endif
    }else if (id == 1){
#ifdef USE_MUIL_THREAD
        {
            bptree_nvm1->Enque_request(&req);
            unique_lock<mutex> lk(req.req_mutex);
            while(!req.finished) {
                req.signal.wait(lk);
            }
        }
#else
        bptree_nvm1->Insert(char8toint64(key.c_str()), value);
#endif
    }else if (id == 2){
#ifdef USE_MUIL_THREAD
        {
            bptree_nvm2->Enque_request(&req);
            unique_lock<mutex> lk(req.req_mutex);
            while(!req.finished) {
                req.signal.wait(lk);
            }
        }
#else
        bptree_nvm2->Insert(char8toint64(key.c_str()), value);
#endif
    }
    else        //其它aep
    {
        if ( flush_size >= FLUSH_SIZE)   //触发倒盘
        {
            Dmark = 1;
            flush_num++;
            flush_size = 0;                //重新计数
            // thread w3(Write_Log, 3);
            // w3.detach();
            Write_Log();
        }
#ifdef USE_MUIL_THREAD
        {
            dram_bptree3->Enque_request(&req);
            unique_lock<mutex> lk(req.req_mutex);
            while(!req.finished) {
                req.signal.wait(lk);
            }
        }
#else
        dram_bptree3->Insert(char8toint64(key.c_str()), value);
#endif
        current_size++; 
        flush_size++;
    }
}

static long get_count = 0;
string aepsystem::Get(const std::string& key) 
{
    string tmp_value;
    int id = Find_aep(key);
    get_count++;
    request req;
    req.key = key;
    req.flag = REQ_GET;
    req.finished = false;
    if(id == 0)  // primary aep
    {
#ifdef USE_MUIL_THREAD
            {
                bptree_nvm0->Enque_request(&req);
                unique_lock<mutex> lk(req.req_mutex);
                while(!req.finished) {
                    req.signal.wait(lk);
                }
            }
            tmp_value =  req.value;
#else
            tmp_value = bptree_nvm0->Get(char8toint64(key.c_str()));
#endif
            if(tmp_value.size() == 0){
                not_find++;
                return "";
            }
            nvm0_find++;
            return tmp_value;
    }else if(id == 1)  // primary aep
    {
#ifdef USE_MUIL_THREAD
            {
                bptree_nvm1->Enque_request(&req);
                unique_lock<mutex> lk(req.req_mutex);
                while(!req.finished) {
                    req.signal.wait(lk);
                }
            }
            tmp_value =  req.value;
#else
            tmp_value = bptree_nvm1->Get(char8toint64(key.c_str()));
#endif
            if(tmp_value.size() == 0){
                not_find++;
                return "";
            }
            nvm1_find++;
            return tmp_value;
    }else if(id == 2)  // primary aep
    {
#ifdef USE_MUIL_THREAD
            {
                bptree_nvm2->Enque_request(&req);
                unique_lock<mutex> lk(req.req_mutex);
                while(!req.finished) {
                    req.signal.wait(lk);
                }
            }
            tmp_value =  req.value;
#else
            tmp_value = bptree_nvm2->Get(char8toint64(key.c_str()));
#endif
            if(tmp_value.size() == 0){
                not_find++;
                return "";
            }
            nvm2_find++;
            return tmp_value;
    }
    else        //其它aep
    {
#ifdef USE_MUIL_THREAD
        {
            dram_bptree3->Enque_request(&req);
            unique_lock<mutex> lk(req.req_mutex);
            while(!req.finished) {
                req.signal.wait(lk);
            }
        }
        tmp_value =  req.value;
#else
        tmp_value = dram_bptree3->Get(char8toint64(key.c_str()));
#endif
         // 没找到  在其它aep中找，并触发预取
        if(tmp_value.size() == 0) {
            if (Dmark && is_cache) //至少经历一次倒盘
            {
                thread r3(Read_Cache, 3);
                r3.join();
            }
            request req;
            req.key = key;
            req.flag = REQ_GETC;
            req.finished = false;
#ifdef USE_MUIL_THREAD
            {
                dram_bptree3->Enque_request(&req);
                unique_lock<mutex> lk(req.req_mutex);
                while(!req.finished) {
                    req.signal.wait(lk);
                }
            }
            tmp_value =  req.value;
#else
            gettimeofday(&be1, NULL);
            tmp_value = dram_bptree3->bptree_nvm->Get(char8toint64(key.c_str()));
            gettimeofday(&en1, NULL);
            nvm3_gtime += (en1.tv_sec-be1.tv_sec) + (en1.tv_usec-be1.tv_usec)/1000000.0;
#endif
            if(tmp_value.size() == 0){
                not_find++;
                return "";
            }
            nvm3_find++;
            return tmp_value;
        }
        else{
            dram_find++;
            return tmp_value;
        }
    }
}

void aepsystem::Delete(const std::string& key)
{
}

void aepsystem::InsertOver(){
    dram_bptree3->InsertOver();
}

aepsystem::aepsystem(){
    is_cache = 0;
    cache_size = 1;
    buf_size = KEY_SIZE + VALUE_SIZE + 1;
}
aepsystem::~aepsystem(){
    delete bptree_nvm0;
    delete bptree_nvm1;
    delete bptree_nvm2;
    delete dram_bptree3;
}

void aepsystem::Initialize()
{
    
    OUT_SIZE = num_size * 0.2;
    // FLUSH_SIZE = OUT_SIZE / 2;
    FLUSH_SIZE = num_size * 0.15;
    OUT_DATA = OUT_SIZE / 20;
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
    dram_bptree3 = new RAMBtree();
    dram_bptree3->Initial(CACHE3, CACHE_SIZE, PATH3, NVM_SIZE, VALUEPATH3, NVM_VALUE_SIZE);
    pthread_t t2;
    if(pthread_create(&t2, NULL, Data_out, NULL) == -1){
        puts("fail to create pthread t0");
        exit(1);
    }
    pthread_detach(t2);
}

void aepsystem::End()
{
    stop = 0;
    cout <<" ----------------Size ---------------" << endl;
    cout << "[SIZE] FLUSH_SIZE: " << FLUSH_SIZE << endl;
    cout << "[SIZE] OUT_SIZE: " << OUT_SIZE << endl;
    cout << "[SIZE] OUT_DATA: " << OUT_DATA << endl;
    cout << "[SIZE] READ_DATA: " << READ_DATA << endl;
    cout << "[COUNT] insert_count: "  << insert_count << endl;
    cout << "[COUNT] get_count: "  << get_count << endl;
    cout <<" ----------------Num ---------------" << endl;
    cout << "[NUM] out_num: " << out_num << endl;
    cout << "[NUM] flush_num: " << flush_num << endl;
    cout << "[NUM] cache_num: " << cache_num << endl;
    cout << "[NUM] cache3_num: " << dram_bptree3->cache_num << endl;
    cout <<" ----------------Time ---------------" << endl;
    cout << "[GET] not_find: "  << not_find << endl;
    cout << "[GET] dram_find: "  << dram_find << endl;
    cout << "[GET] nvm0_find: "  << nvm0_find << endl;
    cout << "[GET] nvm1_find: "  << nvm1_find << endl;
    cout << "[GET] nvm2_find: "  << nvm2_find << endl;
    cout << "[GET] nvm3_find: "  << nvm3_find << endl;
    cout << "[GET] nvm3_insert_time: "  << dram_bptree3->itime << endl;
    cout << "[GET] nvm3_get_time: "     << dram_bptree3->gtime << endl;
    cout << "[GET] nvm3_flush_time: "   << dram_bptree3->ftime << endl;
    cout << "[GET] nvm3_out_time: "     << dram_bptree3->otime << endl;
    cout << "[GET] nvm3_cache_time: "   << dram_bptree3->ctime << endl;
    cout <<" ----------------Result ---------------" << endl;
    cout << dram_find << endl;
    cout << nvm0_find << endl;
    cout << bptree_nvm0->itime << endl;
    cout << bptree_nvm0->gtime << endl;
    cout << nvm1_find << endl;
    cout << bptree_nvm1->itime << endl;
    cout << bptree_nvm1->gtime << endl;
    cout << nvm2_find << endl;
    cout << bptree_nvm2->itime << endl;
    cout << bptree_nvm2->gtime << endl;
    cout << nvm3_find << endl;
    cout << dram_bptree3->ftime << endl;
    cout << dram_bptree3->nvm_gtime << endl;
    cout << dram_bptree3->ctime << endl;
}

void aepsystem::Print()
{}