#include "hashtable.h"
// #include "nvm_common2.h"
#include <iostream>
using namespace std;

// HashTable<Keyvalue> cache_table(1000);

int main(){
    size_t KEY_SIZE = rocksdb::NVM_KeySize; 
    size_t VALUE_SIZE = rocksdb::NVM_ValueSize;
    int i;
    int ops = 100;
    // db_->Initialize();
    char keybuf[KEY_SIZE + 1];
    char valuebuf[VALUE_SIZE + 1];

    snprintf(keybuf, sizeof(keybuf), "%07d", 0);
    for(i = 0; i < ops; i ++){
        cout << i << "value: " << keybuf[6] << endl;
        cout << i << "value: " << keybuf[6] - '0' << endl;
        keybuf[6]++;

    }
    // printf("******Test Start.******\n");
    // for(i = 0; i < ops; i ++) {
    //     snprintf(keybuf, sizeof(keybuf), "%07d", i);
    //     snprintf(valuebuf, sizeof(valuebuf), "%020d", i * i);
    //     string data(keybuf, KEY_SIZE);
    //     string value(valuebuf, VALUE_SIZE);
    //     cout << i << " insert! [Debug] key: " << data << "[Debug] value:" << value << endl;
    //     Keyvalue data1(data, value);
    //     cache_table.insert(data1);
    // }
    // printf("******Insert test finished.******\n");
    // for(i = 0; i < ops; i ++) 
    // {   
    //     snprintf(keybuf, sizeof(keybuf), "%07d", i);
    //     snprintf(valuebuf, sizeof(valuebuf), "%020d", i * i);
    //     string data(keybuf, KEY_SIZE);
    //     string value(valuebuf, VALUE_SIZE);
    //     Keyvalue keyv(data, "");
    //     string tmp_value = cache_table.find_key(keyv);
    //     cout << i << " get! [Debug] key: " << data << "[Debug] value:" << tmp_value << endl;
    //     if(tmp_value.size() == 0) {
    //         printf("Error: Get key-value faild.(key:%s)\n", data.c_str());
    //     } else if(strncmp(value.c_str(), tmp_value.c_str(), VALUE_SIZE) != 0) {
    //         printf("Error: Get key-value faild.(Expect:%s, but Get %s)\n", value.c_str(), tmp_value.c_str());
    //     }  
    // }
    // printf("******Get1 test finished.*****\n");
}