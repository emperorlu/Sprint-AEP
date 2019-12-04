#pragma once

#include <condition_variable>
#include "nvm_common2.h"
using namespace std;

#define REQ_PUT 1
#define REQ_GET 2
#define REQ_OUT 3
#define REQ_CACHE 4
#define REQ_FLUSH 5
#define REQ_DELETE 6

struct request {
    int flag;
    string key;
    string value;
    mutex req_mutex;
    size_t outdata;
    size_t out;
    size_t read;
    condition_variable signal;
    bool finished;
};
