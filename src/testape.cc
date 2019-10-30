#include "nvm_common2.h"
#include <cstring>

using namespace std;
using namespace rocksdb;

int main()
{
    size_t KEY_SIZE = rocksdb::NVM_KeySize;         // 32B
    size_t VALUE_SIZE = rocksdb::NVM_ValueSize;
    int i;
    int ops = 1000;
    char keybuf[KEY_SIZE + 1];
    
    for(int i = 0; i < 10; i++) 
    {   
        snprintf(keybuf, sizeof(keybuf), "%07d", i);
        string data(keybuf, KEY_SIZE);
        char keybuf[NVM_KeyBuf + 1];
        char sign[NVM_SignSize + 1];
        memcpy(keybuf, data.c_str(), data.size());
        snprintf(sign, sizeof(sign), "%07d", 1000000);
        string signdata(sign, NVM_SignSize);
        memcpy(keybuf + NVM_KeySize + NVM_PointSize, signdata.c_str(), NVM_SignSize);
        string tmp_key(keybuf, NVM_KeyBuf);
        cout << "before insert Key: " << tmp_key << endl;
        int len = tmp_key.length();
        int hot = stoi(tmp_key.substr(len-6));
        cout << "before hot: " << hot << endl;
        for(int j = 0; j < ops; j++)
        {
            hot++;
            // tmp_key++;
        }
        tmp_key.replace(len-6, 6, hot.to_string());
        hot = stoi(tmp_key.substr(len-6));
        cout << "after insert Key: " << tmp_key << endl;
        cout << "after hot: " << hot << endl;
    }
    
}
 

