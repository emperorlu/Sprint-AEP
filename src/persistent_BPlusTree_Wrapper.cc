#include "persistent_BPlusTree_Wrapper.h"
// #include <city.h>
namespace rocksdb{
    NVM_BPlusTree_Wrapper::NVM_BPlusTree_Wrapper(){
        allocator_ = nullptr;
        valueAllocator_ = nullptr;
        bptree_ = nullptr;
        // for (int i = 0; i < slots_num_bp; i++) {
        //     bptree_[i] = nullptr;
        // }
    }
    NVM_BPlusTree_Wrapper::~NVM_BPlusTree_Wrapper(){
        if (bptree_ != nullptr){
            delete bptree_;
        }

        if (allocator_ != nullptr){
            delete allocator_;
        }

        if (valueAllocator_ != nullptr){
            delete valueAllocator_;
        }
        // cout << "delete in nvm! " << endl;
        // delete bptree_;
        // delete allocator_;
        // delete valueAllocator_;
        // for (auto list : bptree_) {
        //     delete list;
        // }
    }
    // void NVM_BPlusTree_Wrapper::Initialize(const std::string &path, uint64_t size, int m, size_t key_size, size_t buf_size){
    //     allocator_ = new PersistentAllocator(path, size);
    //     key_size_ = key_size;
    //     buf_size_ = buf_size;
    //     for(auto &tree:bptree_){
    //         tree = new NVMBPlusTree();
    //         tree->Initialize(m,allocator_, key_size_, buf_size_);
    //     }
 
    // }
    void NVM_BPlusTree_Wrapper::Initialize(const std::string &path, uint64_t keysize, const std::string &valuepath, uint64_t valuesize,
                           int m, size_t key_size , size_t buf_size) {
            
        allocator_ = new PersistentAllocator(path, keysize);
        valueAllocator_ = new PersistentAllocator(valuepath, valuesize);
        key_size_ = key_size;
        buf_size_ = buf_size;
        bptree_ = new NVMBpTree();
        bptree_->Initialize(allocator_, valueAllocator_);       
    }

    void NVM_BPlusTree_Wrapper::Insert(const string &key, const string &value){
        bptree_->Insert(key,value);
    }

    void NVM_BPlusTree_Wrapper::Updakey(const string &key){
        bptree_->Updakey(key);
    }

    void NVM_BPlusTree_Wrapper::Insert(const string &key, const string &value, Statistic &stats, size_t which){
        bptree_->Insert(key,value);
        stats.add_entries_num();
    }

    void NVM_BPlusTree_Wrapper::Delete(const std::string& key) {
        bptree_->Delete(key);
    }

    string NVM_BPlusTree_Wrapper::Get(const std::string& key) {
        return bptree_->Get(key);
    }

    void NVM_BPlusTree_Wrapper::GetRange(const std::string key1, const std::string key2, std::vector<std::string> &values, int &size) {
        bptree_->GetRange(key1, key2, values, size);
    }

    void NVM_BPlusTree_Wrapper::FunctionTest(int ops) {
        int i;
        char keybuf[NVM_KeySize + 1];
        char valuebuf[NVM_ValueSize + 1];
        PrintInfo();
        printf("******B+ tree function test start.******\n");
        for(i = 0; i < ops; i ++) {
            snprintf(keybuf, sizeof(keybuf), "%07d", i);
            snprintf(valuebuf, sizeof(valuebuf), "%020d", i * i);
            string data(keybuf, NVM_KeySize);
            string value(valuebuf, NVM_ValueSize);
            Insert(data, value);
        }
        printf("******Insert test finished.******\n");

        for(i = 0; i < 1; i ++) {
            snprintf(keybuf, sizeof(keybuf), "%07d", i);
            snprintf(valuebuf, sizeof(valuebuf), "%020d", i * i);
            string data(keybuf, NVM_KeySize);
            string value(valuebuf, NVM_ValueSize);
            string tmp_value = Get(data);
            // cout << "data:" << data << " value:" << tmp_value << endl;
            if(tmp_value.size() == 0) {
                printf("Error: Get key-value faild.(key:%s)\n", data.c_str());
            } else if(strncmp(value.c_str(), tmp_value.c_str(), NVM_ValueSize) != 0) {
                printf("Error: Get key-value faild.(Expect:%s, but Get %s)\n", value.c_str(), tmp_value.c_str());
            }
        }
        printf("******Get test finished.*****\n");

        // for(i = 0; i < ops / 10; i ++) {
        //     std::vector<std::string>::iterator it;
        //     std::vector<std::string> values;
        //     int getcount = 5;
        //     snprintf(keybuf, sizeof(keybuf), "%07d", i * 10);
        //     string key1(keybuf, NVM_KeySize);
        //     string key2("", 0);
        //     GetRange(key1, key2, values, getcount);
        //     int index = 0;
        //     for(it=values.begin(); it != values.end(); it++) 
        //     {
        //         snprintf(valuebuf, sizeof(valuebuf), "%020d", (i * 10 + index) * (i * 10 + index));
        //         string value(valuebuf, NVM_ValueSize);
        //         if(strncmp(value.c_str(), (*it).c_str(), NVM_ValueSize) != 0) {
        //             printf("Error: Get range key-value faild.(Expect:%s, but Get %s)\n", value.c_str(), (*it).c_str());
        //         }
        //         index ++;
        //     }
        // }
        // printf("******Get range test finished.******\n");

        // for(i = 0; i < ops / 10; i ++) {
        //     snprintf(keybuf, sizeof(keybuf), "%07d", i * 10);
        //     string data(keybuf, NVM_KeySize);
        //     Delete(data);
        // }

        // for(i = ops / 10; i > 0; i --) {
        //     snprintf(keybuf, sizeof(keybuf), "%07d", i * 10 - 1);
        //     string data(keybuf, NVM_KeySize);
        //     Delete(data);
        // }

        // for(i = 0; i < ops / 10; i ++) {
        //     snprintf(keybuf, sizeof(keybuf), "%07d", i * 10);
        //     string data(keybuf, NVM_KeySize);
        //     string tmp_value = Get(data);
        //     if(tmp_value.size() != 0 )  {
        //         printf("Error: Delete key-value faild.(key:%s(%d)),\n", data.c_str());
        //     }
        // }

        // for(i = ops / 10; i > 0; i --) {
        //     snprintf(keybuf, sizeof(keybuf), "%07d", i * 10 - 1);
        //     string data(keybuf, NVM_KeySize);
        //     string tmp_value = Get(data);
        //     if(tmp_value.size() != 0 )  {
        //         printf("Error: Delete key-value faild.(key:%s(%d)),\n", data.c_str());
        //     }
        // }

        // for(i = 0; i <= ops / 2; i ++) {
        //     snprintf(keybuf, sizeof(keybuf), "%07d", i);
        //     string data(keybuf, NVM_KeySize);
        //     Delete(data);
        // }
        // for(i = ops; i > ops / 2; i --) {
        //     snprintf(keybuf, sizeof(keybuf), "%07d", i);
        //     string data(keybuf, NVM_KeySize);
        //     Delete(data);
        // }
        printf("******Delete test finished.******\n");
        printf("******B+ tree function test finished.******\n");
        Print();
        PrintInfo();
    }
}