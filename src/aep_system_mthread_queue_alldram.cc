#include "aep_system.h"
#include <sys/time.h>

using namespace rocksdb;

// NVMBtree *bptree_nvm0;
// NVMBtree *bptree_nvm1;
// NVMBtree *bptree_nvm2;
// NVMBtree *bptree_nvm3;

RAMBtree *dram_bptree0;
RAMBtree *dram_bptree1;
RAMBtree *dram_bptree2;
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


const size_t OPEN_T1 = 100;
const size_t OPEN_T2 = 200;
const size_t OPEN_T3 = 300;


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
    // emp_table.insert(e1);
    // emp_table.insert(e2);
    // emp_table.insert(e3);
    // emp_table.insert(e4);
    return emp_table.f_key(key).getValue();
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
    switch (id)
    { 
        case 0:
            {
                dram_bptree0->Enque_request(&req);
                unique_lock<mutex> lk(req.req_mutex);
                while(!req.finished) {
                    req.signal.wait(lk);
                }
            }
            break;
        case 1:
            {
                dram_bptree1->Enque_request(&req);
                unique_lock<mutex> lk(req.req_mutex);
                while(!req.finished) {
                    req.signal.wait(lk);
                }
            }
            break;
        case 2:
            {
                dram_bptree2->Enque_request(&req);
                unique_lock<mutex> lk(req.req_mutex);
                while(!req.finished) {
                    req.signal.wait(lk);
                }
            }
            break;
        case 3:
            {
                dram_bptree3->Enque_request(&req);
                unique_lock<mutex> lk(req.req_mutex);
                while(!req.finished) {
                    req.signal.wait(lk);
                }
            }
            break;
        default:
            cout << "error!" << endl;
            break;
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
    switch (id)
    {
        case 0:
            {
                dram_bptree0->Enque_request(&req);
                unique_lock<mutex> lk(req.req_mutex);
                while(!req.finished) {
                    req.signal.wait(lk);
                }
            }
            tmp_value =  req.value;
            break;
        case 1:
            {
                dram_bptree1->Enque_request(&req);
                unique_lock<mutex> lk(req.req_mutex);
                while(!req.finished) {
                    req.signal.wait(lk);
                }
            }
            tmp_value =  req.value;
            break;
        case 2:
            {
                dram_bptree2->Enque_request(&req);
                unique_lock<mutex> lk(req.req_mutex);
                while(!req.finished) {
                    req.signal.wait(lk);
                }
            }
            tmp_value =  req.value;
            break;
        case 3:
            {
                dram_bptree3->Enque_request(&req);
                unique_lock<mutex> lk(req.req_mutex);
                while(!req.finished) {
                    req.signal.wait(lk);
                }
            }
            tmp_value =  req.value;
            break;
        default:
            cout << "error!" << endl;
            return "";
    }
    if(tmp_value.size() == 0) {
        not_find++;
        return "";
    }
    else{
        dram_find++;
        return tmp_value;
    }
}

void aepsystem::Delete(const std::string& key)
{
}

aepsystem::aepsystem(){
    is_cache = 0;
    cache_size = 1;
    buf_size = KEY_SIZE + VALUE_SIZE + 1;
    emp_table.insert(e1);
    emp_table.insert(e2);
    emp_table.insert(e3);
    emp_table.insert(e4);
    // one = buf_size;
}
aepsystem::~aepsystem(){
    delete dram_bptree0;
    delete dram_bptree1;
    delete dram_bptree2;
    delete dram_bptree3;
}

void aepsystem::Initialize()
{
    
    cout << "System run!" << endl;

    dram_bptree0 = new RAMBtree();
    dram_bptree0->Initial(CACHE0, CACHE_SIZE, PATH0, NVM_SIZE, VALUEPATH0, NVM_VALUE_SIZE);
    dram_bptree1 = new RAMBtree();
    dram_bptree1->Initial(CACHE1, CACHE_SIZE, PATH1, NVM_SIZE, VALUEPATH1, NVM_VALUE_SIZE);
    dram_bptree2 = new RAMBtree();
    dram_bptree2->Initial(CACHE2, CACHE_SIZE, PATH2, NVM_SIZE, VALUEPATH2, NVM_VALUE_SIZE);
    dram_bptree3 = new RAMBtree();
    dram_bptree3->Initial(CACHE3, CACHE_SIZE, PATH3, NVM_SIZE, VALUEPATH3, NVM_VALUE_SIZE);
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
    cout << "[NUM] cache2_num: " << dram_bptree2->cache_num << endl;
    cout << "[NUM] cache3_num: " << dram_bptree3->cache_num << endl;
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
    cout << "[GET] nvm2_find: "  << nvm2_find << endl;
    cout << "[GET] nvm2_insert_time: "  << dram_bptree2->itime << endl;
    cout << "[GET] nvm2_get_time: "     << dram_bptree2->gtime << endl;
    cout << "[GET] nvm2_flush_time: "   << dram_bptree2->ftime << endl;
    cout << "[GET] nvm2_out_time: "     << dram_bptree2->otime << endl;
    cout << "[GET] nvm2_cache_time: "   << dram_bptree2->ctime << endl;
    cout << "[GET] nvm2_current_num: "   << dram_bptree2->current_num << endl;
    cout << "[GET] nvm3_find: "  << nvm3_find << endl;
    cout << "[GET] nvm3_insert_time: "  << dram_bptree3->itime << endl;
    cout << "[GET] nvm3_get_time: "     << dram_bptree3->gtime << endl;
    cout << "[GET] nvm3_flush_time: "   << dram_bptree3->ftime << endl;
    cout << "[GET] nvm3_out_time: "     << dram_bptree3->otime << endl;
    cout << "[GET] nvm3_cache_time: "   << dram_bptree3->ctime << endl;
    cout << "[GET] nvm3_current_num: "   << dram_bptree3->current_num << endl;
    cout <<" ----------------Result ---------------" << endl;
    cout << dram_find << endl;
    cout << nvm0_find << endl;
    cout << nvm1_find << endl;
    cout << dram_bptree1->ftime << endl;
    cout << dram_bptree1->nvm_gtime << endl;
    cout << dram_bptree1->ctime << endl;
    cout << nvm2_find << endl;
    cout << dram_bptree2->ftime << endl;
    cout << dram_bptree2->nvm_gtime << endl;
    cout << dram_bptree2->ctime << endl;
    cout << nvm3_find << endl;
    cout << dram_bptree3->ftime << endl;
    cout << dram_bptree3->nvm_gtime << endl;
    cout << dram_bptree3->ctime << endl;
}

void aepsystem::Print()
{}