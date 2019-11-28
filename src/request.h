#pragma once

#include <condition_variable>
#include "ram_btree.h"
#include "con_btree.h"
#include "nvm_common2.h"
using namespace std;

#define REQ_PUT 1
#define REQ_GET 2
#define REQ_FLUSH 3
#define REQ_BACK 4
#define REQ_DELETE 5

struct request {
    int flag;
    string key;
    string value;
    vector<ram_entry> flushData;
    vector<entry_key_t> backData;
    mutex req_mutex;
    condition_variable signal;
    bool finished;
};
