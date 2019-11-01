#include "aep_system.h"
#include "nvm_common2.h"
#include <sys/time.h>
#include <cstdlib>
using namespace rocksdb;

int main(int argc, char **argv)
{
    is_cache = atoi(argv[1]);
    cout << "is_cache: " << is_cache << endl;
    struct timeval begin1,begin2,end1,end2;
    
    rocksdb::aepsystem *db_;
    size_t KEY_SIZE = rocksdb::NVM_KeySize;         // 32B
    size_t VALUE_SIZE = rocksdb::NVM_ValueSize;
    int i;
    int ops = 1000000;
    db_ = new rocksdb::aepsystem;
    db_->Initialize();
    char keybuf[KEY_SIZE + 1];
    char valuebuf[VALUE_SIZE + 1];
    printf("******Test Start.******\n");
    gettimeofday(&begin1, NULL);
    for(i = 0; i < ops; i ++) {
        snprintf(keybuf, sizeof(keybuf), "%07d", i);
        snprintf(valuebuf, sizeof(valuebuf), "%020d", i * i);
        string data(keybuf, KEY_SIZE);
        string value(valuebuf, VALUE_SIZE);
        db_->Insert(data, value);
    }
    printf("******Insert test finished.******\n");

    // cout << "nvm0: " << endl;
    // bptree_nvm0->Print();
    // cout << "dram1: " << endl;
    // dram_bptree1->Print();
    gettimeofday(&end1, NULL);
    gettimeofday(&begin2, NULL);
    // for(i = 0; i < ops; i++) 
    // {   
    //     // cout << i << endl;
    //     // for (i = 0; i < (ops/100-j); i++){
    //     snprintf(keybuf, sizeof(keybuf), "%07d", i);
    //     snprintf(valuebuf, sizeof(valuebuf), "%020d", i * i);
    //     string data(keybuf, KEY_SIZE);
    //     string value(valuebuf, VALUE_SIZE);
    //     string tmp_value = db_->Get(data);
    //     if(tmp_value.size() == 0) {
    //         printf("Error: Get key-value faild.(key:%s)\n", data.c_str());
    //     } else if(strncmp(value.c_str(), tmp_value.c_str(), VALUE_SIZE) != 0) {
    //         printf("Error: Get key-value faild.(Expect:%s, but Get %s)\n", value.c_str(), tmp_value.c_str());
    //     }
    //     // }
        
    // }
    // for(int k = 0; k < 10; k ++){
    // for(int j = 0; j < ops; j++) 
    // {   
    //     i = rand()%(ops/10) + k * (ops/10);
    //     // cout << j << " : " << i << endl;
    //     // for (i = 0; i < (ops/100-j); i++){
    //     snprintf(keybuf, sizeof(keybuf), "%07d", i);
    //     snprintf(valuebuf, sizeof(valuebuf), "%020d", i * i);
    //     string data(keybuf, KEY_SIZE);
    //     string value(valuebuf, VALUE_SIZE);
    //     string tmp_value = db_->Get(data);
    //     if(tmp_value.size() == 0) {
    //         printf("Error: Get key-value faild.(key:%s)\n", data.c_str());
    //     } else if(strncmp(value.c_str(), tmp_value.c_str(), VALUE_SIZE) != 0) {
    //         printf("Error: Get key-value faild.(Expect:%s, but Get %s)\n", value.c_str(), tmp_value.c_str());
    //     }
    //     // }
        
    // }
    // }

    for(int j = 0; j < ops; j++) 
    {   
        // i = rand()%(ops/10) + k * (ops/10);
        i = rand()%ops;
        // cout << j << " : " << i << endl;
        // for (i = 0; i < (ops/100-j); i++){
        snprintf(keybuf, sizeof(keybuf), "%07d", i);
        snprintf(valuebuf, sizeof(valuebuf), "%020d", i * i);
        string data(keybuf, KEY_SIZE);
        string value(valuebuf, VALUE_SIZE);
        string tmp_value = db_->Get(data);
        if(tmp_value.size() == 0) {
            printf("Error: Get key-value faild.(key:%s)\n", data.c_str());
        } else if(strncmp(value.c_str(), tmp_value.c_str(), VALUE_SIZE) != 0) {
            printf("Error: Get key-value faild.(Expect:%s, but Get %s)\n", value.c_str(), tmp_value.c_str());
        }
        // }
        
    }
    printf("******Get test finished.*****\n");
    db_->End();
    delete db_;
    gettimeofday(&end2, NULL);
    double delta1 = (end1.tv_sec-begin1.tv_sec) + (end1.tv_usec-begin1.tv_usec)/1000000.0;
    printf("end\n Insert 总共时间：%lf s\n",delta1);
    double delta2 = (end2.tv_sec-begin2.tv_sec) + (end2.tv_usec-begin2.tv_usec)/1000000.0;
    printf("end\n Get 总共时间：%lf s\n",delta2);
    return 0; 

}
 

