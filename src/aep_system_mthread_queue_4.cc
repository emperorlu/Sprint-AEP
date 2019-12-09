#include "aep_system.h"
#include <sys/time.h>

using namespace rocksdb;

NVMBtree *bptree_nvm0;
NVMBtree *bptree_nvm1;
NVMBtree *bptree_nvm2;
NVMBtree *bptree_nvm3;

Employee e1("aep0",0);
Employee e2("aep1",1);
Employee e3("aep2",2);
Employee e4("aep3",3);
HashTable<Employee> emp_table(4);


//大小参数
const size_t NVM_SIZE = 10 * (1ULL << 30);               // 45GB
const size_t NVM_VALUE_SIZE = 40 * (1ULL << 30);         // 180GB
const size_t CACHE_SIZE = 20 * (1ULL << 30);         // 180GB


//统计
int not_find = 0;
int dram_find = 0;
int nvm1_find = 0;
int nvm2_find = 0;
int nvm3_find = 0;
int nvm0_find = 0;


int Find_aep(string key)
{
    emp_table.insert(e1);
    emp_table.insert(e2);
    emp_table.insert(e3);
    emp_table.insert(e4);
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
            break;
        case 1:
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
            break;
        case 2:
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
            break;
        case 3:
#ifdef USE_MUIL_THREAD
            {
                bptree_nvm3->Enque_request(&req);
                unique_lock<mutex> lk(req.req_mutex);
                while(!req.finished) {
                    req.signal.wait(lk);
                }
            }
#else
            bptree_nvm3->Insert(char8toint64(key.c_str()), value);
#endif
            break;
        default:
            count << "error!" << endl;
            return;
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
            break;
        case 1:
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
            break;
        case 2:
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
            break;
        case 3:
#ifdef USE_MUIL_THREAD
            {
                bptree_nvm3->Enque_request(&req);
                unique_lock<mutex> lk(req.req_mutex);
                while(!req.finished) {
                    req.signal.wait(lk);
                }
            }
            tmp_value =  req.value;
#else
            tmp_value = bptree_nvm3->Get(char8toint64(key.c_str()));
#endif
            if(tmp_value.size() == 0){
                not_find++;
                return "";
            }
            nvm3_find++;
            return tmp_value;
            break;
        default:
            count << "error!" << endl;
            return;
    }
}

void aepsystem::Delete(const std::string& key)
{
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
    delete bptree_nvm3;
}

void aepsystem::Initialize()
{
    
    cout << "System run!" << endl;

    bptree_nvm0= new NVMBtree();
    bptree_nvm0->Initial(PATH0, NVM_SIZE, VALUEPATH0, NVM_VALUE_SIZE);
    bptree_nvm1= new NVMBtree();
    bptree_nvm1->Initial(PATH1, NVM_SIZE, VALUEPATH1, NVM_VALUE_SIZE);
    bptree_nvm2= new NVMBtree();
    bptree_nvm2->Initial(PATH1, NVM_SIZE, VALUEPATH1, NVM_VALUE_SIZE);
    bptree_nvm3= new NVMBtree();
    bptree_nvm3->Initial(PATH1, NVM_SIZE, VALUEPATH1, NVM_VALUE_SIZE);
}

void aepsystem::End()
{
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
    cout << bptree_nvm3->itime << endl;
    cout << bptree_nvm3->gtime << endl;
}

void aepsystem::Print()
{}