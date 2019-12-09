#include "aep_system.h"
#include <sys/time.h>

using namespace rocksdb;

// rocksdb::NVM_BPlusTree_Wrapper *bptree_nvm0;
// rocksdb::NVM_BPlusTree_Wrapper *bptree_nvm1;
// rocksdb::NVM_BPlusTree_Wrapper *bptree_nvm2;
// rocksdb::NVM_BPlusTree_Wrapper *bptree_nvm3;
NVMBtree *bptree_nvm0;
NVMBtree *bptree_nvm1;
NVMBtree *bptree_nvm2;
NVMBtree *bptree_nvm3;

// rocksdb::DrNVM_BPlusTree_Wrapper *dram_bptree1;
// rocksdb::DrNVM_BPlusTree_Wrapper *dram_bptree2;
// rocksdb::DrNVM_BPlusTree_Wrapper *dram_bptree3;

Employee e1("aep0",0);
Employee e2("aep1",1);
Employee e3("aep2",2);
Employee e4("aep3",3);
HashTable<Employee> emp_table(4);
// HashTable<Keyvalue> cache_table1(3000);
// HashTable<Keyvalue> cache_table2(3000);
// HashTable<Keyvalue> cache_table3(3000);

// vector<string> updakey1;
// vector<string> updakey2;
// vector<string> updakey3;

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
int nvm0_insert = 0;
int nvm1_insert = 0;
int nvm2_insert = 0;
int nvm3_insert = 0;
int workload = 0;

int cache_num = 0;
int flush_num = 0;
int out_num = 0;

int cache_find = 0;

int update_num1 = 0;

size_t cache1_size = 0;
size_t cache2_size = 0;
size_t cache3_size = 0;

double nvm0_itime = 0;
double nvm0_gtime = 0;
double nvm0_ctime = 0;
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


static long insert_count = 0;
void aepsystem::Insert(const string &key, const string &value)
{
    int id = Find_aep(key);
    insert_count++;
    switch (id)
    { 
        case 0:
            bptree_nvm0->Insert(char8toint64(key.c_str()),value);
            nvm0_insert++;
            break;
        case 1:
            bptree_nvm1->Insert(char8toint64(key.c_str()),value);
            nvm1_insert++;
            break;
        case 2:
            bptree_nvm2->Insert(char8toint64(key.c_str()),value); 
            nvm2_insert++;
            break;
        case 3:
            bptree_nvm3->Insert(char8toint64(key.c_str()),value); 
            nvm3_insert++;
            break;
        default:
            cout << "error!" << endl;
    }
}

static long get_count = 0;
string aepsystem::Get(const std::string& key) 
{
    string tmp_value;
    int id = Find_aep(key);
    get_count++;
    switch (id)
    {
        case 0:
            tmp_value = bptree_nvm0->Get(char8toint64(key.c_str()));
            nvm0_find++;
            break;
        case 1:
            tmp_value = bptree_nvm1->Get(char8toint64(key.c_str()));
            nvm1_find++;
            break;
        case 2:
            tmp_value = bptree_nvm2->Get(char8toint64(key.c_str()));
            nvm2_find++;
            break;
        case 3:
            tmp_value = bptree_nvm3->Get(char8toint64(key.c_str()));
            nvm3_find++;
            break;
        default:
            cout << "error!" << endl;
            return "";
    }
    if(tmp_value.size() == 0){
        not_find++;
        return "";
    }
    return tmp_value;
}

void aepsystem::Delete(const std::string& key)
{
    int id = Find_aep(key);
    switch (id)
    { 
        case 0:
            bptree_nvm0->Delete(char8toint64(key.c_str()));
            break;
        case 1:
            bptree_nvm1->Delete(char8toint64(key.c_str()));
            break;
        case 2:
            bptree_nvm2->Delete(char8toint64(key.c_str())); 
            break;
        case 3:
            bptree_nvm3->Delete(char8toint64(key.c_str()));
            break;
        default:
            cout << "error!" << endl;
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
    
}

void aepsystem::Initialize()
{
    
    cout << "System run!" << endl;

    bptree_nvm0= new NVMBtree();
    bptree_nvm0->Initial(PATH0, NVM_SIZE, VALUEPATH0, NVM_VALUE_SIZE);
    bptree_nvm1= new NVMBtree();
    bptree_nvm1->Initial(PATH1, NVM_SIZE, VALUEPATH1, NVM_VALUE_SIZE);
    bptree_nvm2= new NVMBtree();
    bptree_nvm2->Initial(PATH2, NVM_SIZE, VALUEPATH2, NVM_VALUE_SIZE);
    bptree_nvm3= new NVMBtree();
    bptree_nvm3->Initial(PATH3, NVM_SIZE, VALUEPATH3, NVM_VALUE_SIZE);
    // dram_bptree1->CreateChain();
}

void aepsystem::End()
{
    stop = 0;
    cout << "[NUM] out_num: " << out_num << endl;
    cout << "[NUM] cache_num: " << cache_num << endl;
    cout << "[NUM] flush_num: " << flush_num << endl;
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
    cout << "[GET] nvm1_insert: "  << nvm1_insert << endl;
    cout << "[GET] nvm2_insert: "  << nvm2_insert << endl;
    cout << "[GET] nvm3_insert: "  << nvm3_insert << endl;
    cout << "[GET] nvm0_insert: "  << nvm0_insert << endl;
    cout << "[GET] nvm1_all: "  << nvm1_find + nvm1_insert << endl;
    cout << "[GET] nvm2_all: "  << nvm2_find + nvm2_insert << endl;
    cout << "[GET] nvm3_all: "  << nvm3_find + nvm3_insert << endl;
    cout << "[GET] nvm0_all: "  << nvm0_find + nvm0_insert << endl;
    cout << endl;
    cout << "[COUNT] insert_count: "  << insert_count << endl;
    cout << "[COUNT] get_count: "  << get_count << endl;
    cout << "------------- Result: -------------"  << endl;
    cout << nvm0_find << endl;
    cout << nvm0_insert << endl;
    cout << bptree_nvm0->itime << endl;
    cout << bptree_nvm0->gtime << endl;
    cout << nvm1_find << endl;
    cout << nvm1_insert << endl;
    cout << bptree_nvm1->itime << endl;
    cout << bptree_nvm1->gtime << endl;
    cout << nvm2_find << endl;
    cout << nvm2_insert << endl;
    cout << bptree_nvm2->itime << endl;
    cout << bptree_nvm2->gtime << endl;
    cout << nvm3_find << endl;
    cout << nvm3_insert << endl;
    cout << bptree_nvm3->itime << endl;
    cout << bptree_nvm3->gtime << endl;
}

void aepsystem::Print()
{
}