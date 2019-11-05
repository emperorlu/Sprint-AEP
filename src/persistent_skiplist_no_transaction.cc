#include "persistent_skiplist_no_transaction.h"

namespace rocksdb {
    const uint64_t MemReserved = (10 << 20);  // 保留 10M 空间
    PersistentAllocator::PersistentAllocator(const std::string path, uint64_t size) {
        //pmemaddr_ = static_cast<char *>(pmem_map_file(path.c_str(), size, PMEM_FILE_CREATE | PMEM_FILE_EXCL, 0666, &mapped_len_, &is_pmem_));
        //映射NVM空间到文件
        pmemaddr_ = static_cast<char *>(pmem_map_file(path.c_str(), size, PMEM_FILE_CREATE, 0666, &mapped_len_, &is_pmem_));
            
        if (pmemaddr_ == NULL) {
            printf("PersistentAllocator: map error, filepath %s\n", path.c_str());
            exit(-1);
        } else {
            printf("PersistentAllocator: map at %p \n", pmemaddr_);
        }
        capacity_ = size;
        memused = 0;
        cur_index_ = pmemaddr_;
    }

    PersistentAllocator::~PersistentAllocator() {
        // printf("~PersistentAllocator: unmap at %p \n", pmemaddr_);

        pmem_unmap(pmemaddr_, mapped_len_);
    }

    char* PersistentAllocator::Allocate(size_t bytes) {
        char *result = cur_index_;
        cur_index_ += bytes;
        memused += bytes;
        if(memused > capacity_) {
            printf("%s: NVM full %p\n", __FUNCTION__, pmemaddr_);
            return nullptr;
        }
        return result;
    }

    char* PersistentAllocator::AllocateAligned(size_t bytes, size_t huge_page_size) {
        char* result = cur_index_;
        cur_index_ += bytes;
        memused += bytes;
        return result;
    }

    void PersistentAllocator::ResetZero() {
        pmem_memset_persist(pmemaddr_, 0, mapped_len_);
        // pmem_memset_nodrain(pmemaddr_, 0, mapped_len_);
    }

    size_t PersistentAllocator::BlockSize() {
        return 0;
    }
 
    void PersistentAllocator::PrintStorage(void) {
        printf("Storage capacity is %dG %dM %dK %dB\n", capacity_ >> 30, capacity_ >> 20 & (1024 - 1),
                         capacity_ >> 10 & (1024 - 1), capacity_ & (1024 - 1));
        printf("Storage used is %dG %dM %dK %dB\n", memused >> 30, memused >> 20 & (1024 - 1), 
                        memused >> 10 & (1024 - 1), memused & (1024 - 1));
    }

    bool PersistentAllocator::StorageIsFull() {
        return memused + MemReserved >= capacity_;
    }
}
