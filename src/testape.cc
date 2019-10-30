#include "nvm_common2.h"
#include <cstring>

using namespace std;
using namespace rocksdb;

int main()
{
    size_t KEY_SIZE = rocksdb::NVM_KeySize;         // 32B
    size_t VALUE_SIZE = rocksdb::NVM_ValueSize;
    int i;
    int ops = 10000;
    char keybuf[KEY_SIZE + 1];
    char m_key[10][NVM_KeyBuf];
    for(int i = 0; i < 10; i++) 
    {   
        snprintf(keybuf, sizeof(keybuf), "%07d", i);
        string data(keybuf, KEY_SIZE);
        char keybuf[NVM_KeyBuf + 1];
        char sign[NVM_SignSize + 1];
        memcpy(keybuf, data.c_str(), data.size());
        snprintf(sign, sizeof(sign), "%07d", 0000000);
        string signdata(sign, NVM_SignSize);
        memcpy(keybuf + NVM_KeySize + NVM_PointSize, signdata.c_str(), NVM_SignSize);
        string tmp_key(keybuf, NVM_KeyBuf);
        cout << "tmp_key: " << tmp_key << endl;
        memcpy(m_key[i], tmp_key.c_str(), NVM_KeyBuf);
        // strncpy(m_key[i], tmp_key.c_str(), NVM_KeyBuf);
        cout << "m_key[i][NVM_KeyBuf-8]: " << m_key[i][NVM_KeyBuf-8] << endl;
        printf("b: %s\n;", m_key[i]);
        m_key[i][NVM_KeyBuf-8] = '1';
        string tmp(m_key[i], NVM_KeyBuf);
        cout << "before insert Key: " << tmp << endl;
        
        int len = tmp.length();
        int hot = stoi(tmp.substr(len-6));
        cout << "before hot: " << hot << endl;
        for(int j = 0; j < ops; j++)
        {
            hot++;
            if(j>ops/8){
                if(tmp[len-8] == '1'){
                    cout << "1" << endl;
                    tmp[len-8] = '0';
                    j = ops;
                }
            }
            // tmp_key++;
        }
        tmp.replace(len-6, 6, to_string(hot));
        hot = stoi(tmp.substr(len-6));
        cout << "after insert Key: " << tmp << endl;
        cout << "after hot: " << hot << endl;
        memcpy(m_key[i], tmp.c_str(), NVM_KeyBuf);
        printf("a: %s\n;", m_key[i]);
    }
    
}
 

