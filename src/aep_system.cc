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

rocksdb::DrNVM_BPlusTree_Wrapper *dram_bptree1;
rocksdb::DrNVM_BPlusTree_Wrapper *dram_bptree2;
rocksdb::DrNVM_BPlusTree_Wrapper *dram_bptree3;

Employee e1("aep0",0);
Employee e2("aep1",1);
Employee e3("aep2",2);
Employee e4("aep3",3);
HashTable<Employee> emp_table(4);
// HashTable<Keyvalue> cache_table1(3000);
// HashTable<Keyvalue> cache_table2(3000);
// HashTable<Keyvalue> cache_table3(3000);

vector<string> updakey1;
vector<string> updakey2;
vector<string> updakey3;

//大小参数
const size_t NVM_SIZE = 10 * (1ULL << 30);               // 45GB
const size_t NVM_VALUE_SIZE = 40 * (1ULL << 30);         // 180GB
const size_t CACHE_SIZE = 20 * (1ULL << 30);         // 180GB

//阈值
const size_t OUT_DATA = 10000;
const size_t READ_DATA = 5000;

const size_t OPEN_T1 = 100;
const size_t OPEN_T2 = 200;
const size_t OPEN_T3 = 300;

const size_t FLUSH_SIZE = 60 * (1ULL << 20);
const size_t OUT_SIZE = 600 * (1ULL << 20);
// const size_t FLUSH_SIZE = 3000;
// const size_t OUT_SIZE = 60000;


//标记
int Dmark = 0;
int stop = 1;
int one = 1;
int open = 1;

//统计
int current_size = 0;
int flush_size = 0;
int not_find = 0;
int dram_find = 0;
int nvm_find = 0;
int nvm0_find = 0;
int workload = 0;

int cache_num = 0;
int flush_num = 0;
int out_num = 0;

int cache_find = 0;

int update_num1 = 0;

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

// void Read_Cache(){
//     // cout << "begin cache! " << endl;  
//     bptree_nvm1->CreateChain();
//     vector<string> backData1;
//     size_t read = READ_DATA;
//     backData1 = bptree_nvm1->BacktoDram(dram_bptree1->GetMinHot(), read);
//     // cout << "size1: " << backData1.size() << endl;
//     if(backData1.size()!=0){
//         for(int i=0;i<backData1.size();i++){
//             if(bptree_nvm1->Get(backData1[i]).size() == 0)
//                 cout << "cache data empty " << endl;
//             else{
//                 Keyvalue data1(backData1[i], bptree_nvm1->Get(backData1[i]));
//                 cache_table1.insert(data1);
//             }
                
//         }
//     }
//     // cout << "cache1 size: ";
//     // cout << cache_table1.getSize() << endl;
//     backData1.clear();

//     //aep2   
//     bptree_nvm2->CreateChain();
//     vector<string> backData2;
//     backData2 = bptree_nvm2->BacktoDram(dram_bptree2->GetMinHot(), read);
//     // cout << "size2: " << backData2.size() << endl;
//     if(backData2.size()!=0){
//         for(int i=0;i<backData2.size();i++){
//             if((bptree_nvm2->Get(backData2[i])).size() == 0)
//                 cout << "cache2 data empty " << endl;
//             else{
//                 // cout << "backData2[i]: " << backData2[i] << endl;
//                 Keyvalue data2(backData2[i], bptree_nvm2->Get(backData2[i]));
//                 cache_table2.insert(data2);
//             }
//         }
//     }
//     // cout << "cache2 size: ";
//     // cout << cache_table2.getSize() << endl;
//     backData2.clear();

    
//     //aep3
//     bptree_nvm3->CreateChain();
//     vector<string> backData3;
//     backData3 = bptree_nvm3->BacktoDram(dram_bptree3->GetMinHot(), read);
//     // cout << "size3: " << backData3.size() << endl;
//     if(backData3.size()!=0){
//         for(int i=0;i<backData3.size();i++){
//             Keyvalue data3(backData3[i], bptree_nvm3->Get(backData3[i]));
//             cache_table3.insert(data3);
//         }
//     }
//     // cout << "cache3 size: ";
//     // cout << cache_table3.getSize() << endl;
//     backData3.clear();
// }

void* Data_out(void *arg) 
{
    while(stop){
        if((current_size * one) >= OUT_SIZE && Dmark)
        {
            
            
            // std::lock_guard<std::mutex> lk(m_mutex);
            m_mutex.lock();
            // cout << "[DEBUG] Begin out data!" << endl;
            // cout << "[DEBUG] current_size:" << current_size << endl;
            out_num++;
            vector<string> outData;
            size_t out = OUT_DATA;
            // dram_bptree1->CreateChain();
            outData = dram_bptree1->OutdeData(out);
            // cout << "outData.size(): " << outData.size() << endl;
            if(outData.size()!=0){
                for(int i=0;i<outData.size();i++){
                    dram_bptree1->Delete(outData[i]);
                    updakey1.push_back(outData[i]);
                    current_size--;
                }
            }
            update_num1 += updakey1.size();

            vector<string> outData2;
            // dram_bptree2->CreateChain();
            outData2 = dram_bptree2->OutdeData(out);
            if(outData2.size()!=0){
                for(int i=0;i<outData2.size();i++){
                    dram_bptree2->Delete(outData2[i]);
                    updakey2.push_back(outData2[i]);
                    current_size--;
                }
            }

            vector<string> outData3;
            // dram_bptree3->CreateChain();
            outData3 = dram_bptree3->OutdeData(out);
            if(outData3.size()!=0){
                for(int i=0;i<outData3.size();i++){
                    dram_bptree3->Delete(outData3[i]);
                    updakey3.push_back(outData3[i]);
                    current_size--;
                }
            }
            flush_size = current_size;
            m_mutex.unlock();
        }
    }
    pthread_exit(NULL);
}


void Read_Cache()     //预取
{     
    cache_num++;
    //aep1
    // bptree_nvm1->CreateChain();
    vector<string> backData1;
    size_t read = READ_DATA;
    backData1 = bptree_nvm1->BacktoDram(dram_bptree1->MinHot(), read);
    cout << "size1: " << backData1.size();
    if(backData1.size()!=0){
        for(int i=0;i<backData1.size();i++){
            dram_bptree1->Insert(backData1[i], bptree_nvm1->Get(char8toint64(backData1[i].c_str())));
        }
    }
    backData1.clear();

    //aep2   
    // bptree_nvm2->CreateChain();
    vector<string> backData2;
    backData2 = bptree_nvm2->BacktoDram(dram_bptree2->MinHot(), read);
    cout << "size2: " << backData2.size();
    if(backData2.size()!=0){
        for(int i=0;i<backData2.size();i++){
            dram_bptree2->Insert(backData2[i], bptree_nvm2->Get(char8toint64(backData2[i].c_str())));
        }
    }
    backData2.clear();
    // gettimeofday(&end2, NULL);
    // double delta2 = (end2.tv_sec-begin.tv_sec) + (end2.tv_usec-begin.tv_usec)/1000000.0;
    // printf("end\n cache2 timeval 总共时间：%lf us\n",delta2);
    
    //aep3
    // bptree_nvm3->CreateChain();
    vector<string> backData3;
    backData3 = bptree_nvm3->BacktoDram(dram_bptree3->MinHot(), read);
    cout << "size3: " << backData3.size() << endl;
    if(backData3.size()!=0){
        for(int i=0;i<backData3.size();i++){
            dram_bptree3->Insert(backData3[i], bptree_nvm3->Get(char8toint64(backData3[i].c_str())));
        }
    }
    backData3.clear();
    // gettimeofday(&end3, NULL);
    // double delta3 = (end3.tv_sec-begin.tv_sec) + (end3.tv_usec-begin.tv_usec)/1000000.0;
    // printf("end\n cache3 timeval 总共时间：%lf us\n",delta3);

    // m_mutex.unlock();
    // pthread_exit(NULL);
}

void Write_Log()    //倒盘
{   
    // std::lock_guard<std::mutex> lk(m_mutex);
    // m_mutex.lock();
    // cout << "[DEBUG] Begin write log!" << endl;
    //aep1
    vector<string> insertData1;
    insertData1 = dram_bptree1->FlushtoNvm();
    // cout << "flush size: " << insertData1.size() << endl;
    for(int i=0;i<insertData1.size();i++){
        int len = insertData1[i].length();
        uint64_t hot = stoi(insertData1[i].substr(len-7, NVM_SignSize-1));
        // bptree_nvm1->Insert(char8toint64(insertData1[i].c_str()), hot, dram_bptree1->Geti(insertData1[i]));
        bptree_nvm1->Insert(char8toint64(insertData1[i].c_str()), hot, dram_bptree1->Geti(insertData1[i]));
        // bptree_nvm1->Insert(char8toint64(insertData1[i].c_str()), dram_bptree1->Get(insertData1[i]));
    }
    // cout << "update size: " << updakey1.size() << endl;
    for(int i=0;i<updakey1.size();i++){
        bptree_nvm1->Updakey(updakey1[i]);
    }
    updakey1.clear(); 

    //aep2
    vector<string> insertData2;
    insertData2 = dram_bptree2->FlushtoNvm();
    for(int i=0;i<insertData2.size();i++){
        // if (dram_bptree2->Get(insertData2[i]).size() != 0)
        //     bptree_nvm2->Insert(char8toint64(insertData2[i].c_str()), dram_bptree2->Get(insertData2[i]));
        int len = insertData2[i].length();
        uint64_t hot = stoi(insertData2[i].substr(len-7, NVM_SignSize-1));
        bptree_nvm2->Insert(char8toint64(insertData2[i].c_str()), hot, dram_bptree2->Geti(insertData2[i]));
    }
    for(int i=0;i<updakey2.size();i++){
        bptree_nvm2->Updakey(updakey2[i]);
    }
    updakey2.clear();

    //aep3
    vector<string> insertData3;
    insertData3 = dram_bptree3->FlushtoNvm();
    for(int i=0;i<insertData3.size();i++){
        int len = insertData3[i].length();
        uint64_t hot = stoi(insertData3[i].substr(len-7, NVM_SignSize-1));
        bptree_nvm3->Insert(char8toint64(insertData3[i].c_str()), hot, dram_bptree3->Geti(insertData3[i]));
    }
    for(int i=0;i<updakey3.size();i++){
        bptree_nvm3->Updakey(updakey3[i]);
    }
    updakey3.clear();


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
        // bptree_nvm0->Insert(key,value);
    }
    else        //其它aep
    {
        if ( (flush_size * one) >= FLUSH_SIZE)   //触发倒盘
        {
            Dmark = 1;
            flush_num++;
            flush_size = 0;//重新计数
            Write_Log();
        }
        switch (id)
        { 
            case 1:
                dram_bptree1->Insert(key,value);
                current_size++; 
                flush_size++;
                break;
            case 2:
                dram_bptree2->Insert(key,value); 
                current_size++;  
                flush_size++;
                break;
            case 3:
                dram_bptree3->Insert(key,value);  
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
    Keyvalue keyv(key, "");
    m_mutex.lock();
    // std::lock_guard<std::mutex> lk(m_mutex);
    get_count++;
    // cout << "[DEBUG] Get (" << get_count << ") key: " << char8toint64(key.c_str()) << " id: " << id << endl;
    // cout << "[DEBUG] Get (" << get_count << ") key: " << key << endl;
    if(id == 0)  // primary aep
    {
        // tmp_value = bptree_nvm0->Get(key);
        tmp_value = bptree_nvm0->Get(char8toint64(key.c_str()));
        // cout << "[DEBUG] Get in nvm0! " << endl;
        m_mutex.unlock();
        nvm0_find++;
        return tmp_value;
    }
    else        //其它aep
    {
        switch (id)
        {
            case 1:
                // tmp_value = cache_table1.find_key(keyv);
                // // cout << "value: " << tmp_value;
                // if(tmp_value.size() == 0) {
                //     tmp_value = dram_bptree1->Get(key);
                // }
                // else{
                //     cache_find++;
                //     m_mutex.unlock();
                //     return tmp_value;
                // }
                tmp_value = dram_bptree1->Get(key);
                break;
            case 2:
                // tmp_value = cache_table2.find_key(keyv);
                // if(tmp_value.size() == 0) {
                //     tmp_value = dram_bptree2->Get(key);
                // }
                // else{
                //     cache_find++;
                //     m_mutex.unlock();
                //     return tmp_value;
                // }
                tmp_value = dram_bptree2->Get(key);
                break;
            case 3:
                // tmp_value = cache_table3.find_key(keyv);
                // if(tmp_value.size() == 0) {
                //     tmp_value = dram_bptree3->Get(key);
                // }
                // else{
                //     cache_find++;
                //     m_mutex.unlock();
                //     return tmp_value;
                // }
                tmp_value = dram_bptree3->Get(key);
                break;
            default:
                cout << "error!" << endl;
                m_mutex.unlock();
                return "";
        }
         // 没找到  在其它aep中找，并触发预取
        // cout << "get tmp_value!" << endl;
        if(tmp_value.size() == 0) {
            if (Dmark) //至少经历一次倒盘
            {
                // cout << "[DEBUG] Read Cache!" << endl;
                if(is_cache)
                    Read_Cache();
            }
            switch (id)
            {
                case 1:
                    // tmp_value = bptree_nvm1->Get(key);
                    tmp_value = bptree_nvm1->Get(char8toint64(key.c_str()));
                    break;
                case 2:
                    // tmp_value = bptree_nvm2->Get(key);
                    tmp_value = bptree_nvm2->Get(char8toint64(key.c_str()));
                    break;
                case 3:
                    // tmp_value = bptree_nvm3->Get(key);
                    tmp_value = bptree_nvm3->Get(char8toint64(key.c_str()));
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
        // bptree_nvm0->Delete(key);
        bptree_nvm0->Delete(char8toint64(key.c_str()));
    }
    else        //其它aep
    {
        switch (id)
        {
            case 1:
                dram_bptree1->Delete(key); 
                current_size--; 
                flush_size--;
                break;
            case 2:
                dram_bptree2->Delete(key); 
                current_size--;  
                flush_size--;
                break;
            case 3:
                dram_bptree3->Delete(key);  
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
    buf_size = KEY_SIZE + VALUE_SIZE + 1;
    one = buf_size;
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
    
    // bptree_nvm0 = new rocksdb::NVM_BPlusTree_Wrapper();
    // bptree_nvm0->Initialize(PATH0, NVM_SIZE, VALUEPATH0, NVM_VALUE_SIZE, 10, KEY_SIZE, buf_size);

    // bptree_nvm1 = new rocksdb::NVM_BPlusTree_Wrapper();
    // bptree_nvm1->Initialize(PATH1, NVM_SIZE, VALUEPATH1, NVM_VALUE_SIZE, 10, KEY_SIZE, buf_size);
    // bptree_nvm2 = new rocksdb::NVM_BPlusTree_Wrapper();
    // bptree_nvm2->Initialize(PATH2, NVM_SIZE, VALUEPATH2, NVM_VALUE_SIZE, 10, KEY_SIZE, buf_size);
    // bptree_nvm3 = new rocksdb::NVM_BPlusTree_Wrapper();
    // bptree_nvm3->Initialize(PATH3, NVM_SIZE, VALUEPATH2, NVM_VALUE_SIZE, 10, KEY_SIZE, buf_size);
    bptree_nvm0= new NVMBtree();
    bptree_nvm0->Initial(PATH0, NVM_SIZE, VALUEPATH0, NVM_VALUE_SIZE);
    bptree_nvm1= new NVMBtree();
    bptree_nvm1->Initial(PATH1, NVM_SIZE, VALUEPATH1, NVM_VALUE_SIZE);
    bptree_nvm2= new NVMBtree();
    bptree_nvm2->Initial(PATH2, NVM_SIZE, VALUEPATH2, NVM_VALUE_SIZE);
    bptree_nvm3= new NVMBtree();
    bptree_nvm3->Initial(PATH3, NVM_SIZE, VALUEPATH3, NVM_VALUE_SIZE);

    dram_bptree1 = new rocksdb::DrNVM_BPlusTree_Wrapper();
    dram_bptree1->Initialize(CACHE1, CACHE_SIZE, 10, KEY_SIZE, buf_size);
    // dram_bptree1->PrintInfo();
    dram_bptree2 = new rocksdb::DrNVM_BPlusTree_Wrapper();
    dram_bptree2->Initialize(CACHE2, CACHE_SIZE, 10, KEY_SIZE, buf_size);
    dram_bptree3 = new rocksdb::DrNVM_BPlusTree_Wrapper();
    dram_bptree3->Initialize(CACHE3, CACHE_SIZE, 10, KEY_SIZE, buf_size);
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
    cout << "[NUM] flush_num: " << flush_num << endl;

    cout << "[SIZE] current_size: " << current_size << endl;
    cout << "[SIZE] flush_size: " << flush_size << endl;
    cout << "[SIZE] FLUSH_SIZE: " << FLUSH_SIZE << endl;
    cout << "[SIZE] OUT_SIZE: " << OUT_SIZE << endl;

    cout << "[SIZE] OUT_DATA: " << OUT_DATA << endl;
    cout << "[SIZE] READ_DATA: " << READ_DATA << endl;
    cout << "[SIZE] one: " << one << endl;

    // cout << "[GET] cache_find: " << cache_find << endl;
    cout << "[GET] not_find: "  << not_find << endl;
    cout << "[GET] dram_find: "  << dram_find << endl;
    cout << "[GET] nvm_find: "  << nvm_find << endl;
    cout << "[GET] nvm0_find: "  << nvm0_find << endl;

    cout << "[COUNT] insert_count: "  << insert_count << endl;
    cout << "[COUNT] get_count: "  << get_count << endl;

    cout << "[COUNT] update_num1: "  << update_num1 << endl;
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