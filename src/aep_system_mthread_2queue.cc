#include "aep_system.h"
#include <sys/time.h>

using namespace rocksdb;

NVMBtree *bptree_nvm0;

RAMBtree *dram_bptree1;

Employee e1("aep0",0);
Employee e2("aep1",1);
HashTable<Employee> emp_table(2);


//大小参数
const size_t NVM_SIZE = 10 * (1ULL << 30);               // 45GB
const size_t NVM_VALUE_SIZE = 50 * (1ULL << 30);         // 180GB
const size_t CACHE_SIZE = 50 * (1ULL << 30);         // 180GB

// const size_t NVM_SIZE = 5 * (1ULL << 30);               // 45GB
// const size_t NVM_VALUE_SIZE = 30 * (1ULL << 30);         // 180GB
// const size_t CACHE_SIZE = 20 * (1ULL << 30);         // 180GB

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
int nvm0_find = 0;

int cache_num = 0;
int cache1_num = 0;

int flush_num = 0;
int out_num = 0;
double nvm1_itime = 0;
double nvm1_gtime = 0;
double nvm1_ctime = 0;

struct timeval be1,en1;

int Find_aep(string key)
{
    return emp_table.f_key(key).getValue();
}

void* Data_out(void *arg) 
{
    while(stop){
        if(current_size  >= OUT_SIZE && Dmark)
        {
            out_num++;
            request req;
            req.out = OUT_DATA;
            req.flag = REQ_OUT;
            req.finished = false;
            vector<ram_entry_key_t> outData;
            size_t out = OUT_DATA;
            {
                dram_bptree1->Enque_request(&req);
                unique_lock<mutex> lk(req.req_mutex);
                while(!req.finished) {
                    req.signal.wait(lk);
                }
            }
            current_size -= req.outdata;
            flush_size = current_size;
        }
    }
    pthread_exit(NULL);
}

void Read_Cache()     //预取
{     
    cache_num++;
    size_t read = READ_DATA;
    vector<entry_key_t> backData;
    request req;
    req.read = READ_DATA;
    req.flag = REQ_CACHE;
    req.finished = false;
    {
        dram_bptree1->Enque_request(&req);
        unique_lock<mutex> lk(req.req_mutex);
        while(!req.finished) {
            req.signal.wait(lk);
        }
    }
}

void Write_Log()    //倒盘
{   
    vector<ram_entry> insertData;
    request req;
    req.flag = REQ_FLUSH;
    req.finished = false;
    {
        dram_bptree1->Enque_request(&req);
        unique_lock<mutex> lk(req.req_mutex);
        while(!req.finished) {
            req.signal.wait(lk);
        }
    }
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
        {
            bptree_nvm0->Enque_request(&req);
            unique_lock<mutex> lk(req.req_mutex);
            while(!req.finished) {
                req.signal.wait(lk);
            }
        }
    }
    else    //其它aep
    {
        if ( flush_size >= FLUSH_SIZE)   //触发倒盘
        {
            Dmark = 1;
            flush_num++;
            flush_size = 0;                //重新计数
            Write_Log();
        }
        {
            dram_bptree1->Enque_request(&req);
            unique_lock<mutex> lk(req.req_mutex);
            while(!req.finished) {
                req.signal.wait(lk);
            }
        }
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
        {
            bptree_nvm0->Enque_request(&req);
            unique_lock<mutex> lk(req.req_mutex);
            while(!req.finished) {
                req.signal.wait(lk);
            }
        }
        tmp_value =  req.value;
        if(tmp_value.size() == 0){
            not_find++;
            return "";
        }
        nvm0_find++;
        return tmp_value;
    }
    else        //其它aep
    {
        {
            dram_bptree1->Enque_request(&req);
            unique_lock<mutex> lk(req.req_mutex);
            while(!req.finished) {
                req.signal.wait(lk);
            }
        }
        tmp_value =  req.value;
         // 没找到  在其它aep中找，并触发预取
        if(tmp_value.size() == 0) {
            if (Dmark && is_cache) //至少经历一次倒盘
            {
                Read_Cache();
            }
            request req;
            req.key = key;
            req.flag = REQ_GETC;
            req.finished = false;
            {
                dram_bptree1->Enque_request(&req);
                unique_lock<mutex> lk(req.req_mutex);
                while(!req.finished) {
                    req.signal.wait(lk);
                }
            }
            tmp_value =  req.value;
            if(tmp_value.size() == 0){
                not_find++;
                return "";
            }
            nvm1_find++;
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
    int id = Find_aep(key);
    if(id == 0)  // primary aep
    {
        // bptree_nvm0->Delete(char8toint64(key.c_str()));
        bptree_nvm0->Delete(char8toint64(key.c_str()));
    }
    else        //其它aep
    {
        dram_bptree1->Delete(char8toint64(key.c_str())); 
        current_size--; 
        flush_size--;
    }
}

aepsystem::aepsystem(){
    is_cache = 0;
    cache_size = 1;
    buf_size = KEY_SIZE + VALUE_SIZE + 1;
    emp_table.insert(e1);
    emp_table.insert(e2);
}
aepsystem::~aepsystem(){
    delete bptree_nvm0;
    
    delete dram_bptree1;
}

void aepsystem::Initialize()
{
    
    OUT_SIZE = num_size * 0.46;
    FLUSH_SIZE = OUT_SIZE / 2;
    OUT_DATA = OUT_SIZE / 46;
    READ_DATA = OUT_DATA / 100;
    // READ_DATA = 1;
    cout << "System run!" << endl;
    cout << "[SIZE] FLUSH_SIZE: " << FLUSH_SIZE << endl;
    cout << "[SIZE] OUT_SIZE: " << OUT_SIZE << endl;
    cout << "[SIZE] OUT_DATA: " << OUT_DATA << endl;
    cout << "[SIZE] READ_DATA: " << READ_DATA << endl;

    bptree_nvm0= new NVMBtree();
    bptree_nvm0->Initial(PATH2, NVM_SIZE, VALUEPATH2, NVM_VALUE_SIZE);

    dram_bptree1 = new RAMBtree();
    dram_bptree1->Initial(CACHE1, CACHE_SIZE, PATH1, NVM_SIZE, VALUEPATH1, NVM_VALUE_SIZE);

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
    cout << "[NUM] cache1_num: " << dram_bptree1->cache_num << endl;
    cout <<" ----------------Time ---------------" << endl;
    cout << "[GET] not_find: "  << not_find << endl;
    cout << "[GET] dram_find: "  << dram_find << endl;
    cout << "[GET] nvm0_find: "  << nvm0_find << endl;
    cout << "[GET] nvm1_find: "  << nvm1_find << endl;
    cout << "[GET] nvm1_insert_time: "  << dram_bptree1->itime << endl;
    cout << "[GET] nvm1_get_time: "     << dram_bptree1->gtime << endl;
    cout << "[GET] nvm1_flush_time: "   << dram_bptree1->ftime << endl;
    cout << "[GET] nvm1_out_time: "     << dram_bptree1->otime << endl;
    cout << "[GET] nvm1_cache_time: "   << dram_bptree1->ctime << endl;
    cout << "[GET] nvm1_current_num: "   << dram_bptree1->current_num << endl;
    cout <<" ----------------Result ---------------" << endl;
    cout << dram_find << endl;
    cout << nvm0_find << endl;
    cout << nvm1_find << endl;
    cout << dram_bptree1->ftime << endl;
    cout << dram_bptree1->nvm_gtime << endl;
    cout << dram_bptree1->ctime << endl;
}

void aepsystem::Print()
{}