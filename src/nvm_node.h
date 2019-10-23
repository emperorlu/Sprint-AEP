#pragma once

#include <cstring>
#include <string>
#include <cassert>

#include "libpmem.h"
#include "random.h"

namespace rocksdb {
    struct Node {
        explicit Node(const char* key) : key_(key) {
        };
        ~Node() = default;
        
        Node* Next(int n) {
            assert(n >= 0);
            return next_[n];
        }

        void SetNext(int n, Node* next) {
            assert(n >= 0);
            next_[n] = next;
            pmem_persist(next_ + n, sizeof(Node*));
        }
        const char *key_;
        Node* next_[1];
    };
}