#include "nvm_common2.h"
#include "nvm_allocator.h"

NVMAllocator *node_alloc = nullptr;
NVMAllocator *value_alloc = nullptr;

atomic<uint64_t> perist_data(0);

int AllocatorInit(const std::string &path, uint64_t keysize, const std::string &valuepath, 
                uint64_t valuesize) {
    node_alloc = new NVMAllocator(path, keysize);
    if(node_alloc == nullptr) {
        return -1;
    }
    value_alloc = new NVMAllocator(valuepath, valuesize);
    if(value_alloc == nullptr) {
        delete node_alloc;
        return -1;
    }
    // perist_data = 0;
    return 0;
}

void AllocatorExit() {
    if(node_alloc) {
        delete node_alloc;
    } 

    if(value_alloc) {
        delete value_alloc;
    }

}