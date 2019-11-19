/*
   Copyright (c) 2018, UNIST. All rights reserved.  The license is a free
   non-exclusive, non-transferable license to reproduce, use, modify and display
   the source code version of the Software, with or without modifications solely
   for non-commercial research, educational or evaluation purposes. The license
   does not entitle Licensee to technical support, telephone assistance,
   enhancements or updates to the Software. All rights, title to and ownership
   interest in the Software, including all intellectual property rights therein
   shall remain in UNIST. 
*/

#include <unistd.h>
#include <stdio.h>
#include <stdint.h>
#include <time.h>
#include <stdlib.h>
#include <math.h>
#include <iostream>
#include <fstream>
#include <vector>
#include <string.h>
#include <cassert>
#include <climits>
#include <future>
#include <mutex>
#include <vector>
#include <list>
// #include "hashtable.h"

#include "nvm_common2.h"

using namespace rocksdb;

#define RAM_PAGESIZE 256

// #define CPU_FREQ_MHZ (1994)
// #define DELAY_IN_NS (1000)
#define RAM_CACHE_LINE_SIZE 64 
// #define QUERY_NUM 25

#define RAM_IS_FORWARD(c) (c % 2 == 0)

// using ram_entry_key_t = uint64_t;
struct ram_entry_key_t {
    uint64_t key;
    uint64_t hot;
    ram_entry_key_t() :key(ULONG_MAX), hot(0) {}
    ram_entry_key_t(uint64_t key_, uint64_t hot_ = 0) :key(key_), hot(hot_){}
    ram_entry_key_t & operator = (const ram_entry_key_t &ram_entry) {
        if(this == &ram_entry) {
            return *this;
        }

        key = ram_entry.key;
        hot = ram_entry.hot;
        return *this;
    }

    bool operator < (const ram_entry_key_t &ram_entry) {
        return key < ram_entry.key;
    }

    bool operator <= (const ram_entry_key_t &ram_entry) {
        return key <= ram_entry.key;
    }

    bool operator > (const ram_entry_key_t &ram_entry) {
        return key > ram_entry.key;
    }

    bool operator >= (const ram_entry_key_t &ram_entry) {
        return key >= ram_entry.key;
    }

    bool operator == (const ram_entry_key_t &ram_entry) {
        return key == ram_entry.key;
    }

    bool operator != (const ram_entry_key_t &ram_entry) {
        return key != ram_entry.key;
    }

    operator int64_t () {
        return key;
    }

    operator uint64_t () {
        return key;
    }

    operator int () {
        return key;
    }

    operator uint32_t () {
        return key;
    }
};


class ram_node;

class ram_tree{
  private:
    NVMAllocator *node_alloc;
    int height;
    char* root;

  public:

    ram_tree();
    // ram_tree(ram_node *root);
    void btree_init(); 
    ~ram_tree() {
        if(node_alloc) {
            delete node_alloc;
        }
    }
    void *Newram_node();
    void setNewRoot(char *);
    void getNumberOfNodes();
    void btree_insert(ram_entry_key_t, char*);
    void btree_insert_internal(char *, ram_entry_key_t, char *, uint32_t);
    void btree_delete(ram_entry_key_t);
    void btree_delete_internal(ram_entry_key_t, char *, uint32_t, ram_entry_key_t *, bool *, ram_node **);
    char *btree_search(ram_entry_key_t);
    void btree_search_range(ram_entry_key_t, ram_entry_key_t, unsigned long *); 
    void btree_search_range(ram_entry_key_t, ram_entry_key_t, std::vector<std::string> &values, int &size); 
    void printAll();
    void PrintInfo();
    void CalculateSapce(uint64_t &space);

    friend class ram_node;

    // void CreateChain();

    // vector<string> BacktoDram(int hot, size_t read);
};

class ram_header{
  private:
    ram_node* leftmost_ptr;         // 8 bytes
    ram_node* sibling_ptr;          // 8 bytes
    uint32_t level;             // 4 bytes
    uint8_t switch_counter;     // 1 bytes
    uint8_t is_deleted;         // 1 bytes
    int16_t last_index;         // 2 bytes
    // uint64_t hot;
    // uint8_t is_d   
    std::mutex *mtx;      // 8 bytes

    friend class ram_node;
    friend class ram_tree;

  public:
    ram_header() {
      mtx = new std::mutex();

      leftmost_ptr = NULL;  
      sibling_ptr = NULL;
      switch_counter = 0;
      last_index = -1;
      is_deleted = false;
    }

    ~ram_header() {
      delete mtx;
    }
};

class ram_entry{ 
  private:
    ram_entry_key_t key; // 8 bytes
    char* ptr; // 8 bytes

  public:
    ram_entry(){
      key = ram_entry_key_t(ULONG_MAX);
      ptr = NULL;
    }

    friend class ram_node;
    friend class ram_tree;
};

const int ram_cardinality = (RAM_PAGESIZE-sizeof(ram_header))/sizeof(ram_entry);
const int ram_count_in_line = RAM_CACHE_LINE_SIZE / sizeof(ram_entry);

class ram_node{
  private:
    ram_header hdr;  // ram_header in persistent memory, 16 bytes
    ram_entry records[ram_cardinality]; // slots in persistent memory, 16 bytes * n


  public:
    friend class ram_tree;

    ram_node(uint32_t level = 0) {
      hdr.level = level;
      records[0].ptr = NULL;
    }

    // this is called when tree grows
    ram_node(ram_node* left, ram_entry_key_t key, ram_node* right, uint32_t level = 0) {
      hdr.leftmost_ptr = left;  
      hdr.level = level;
      records[0].key = key;
      records[0].ptr = (char*) right;
      records[1].ptr = NULL;

      hdr.last_index = 0;

      // clflush((char*)this, sizeof(ram_node));
    }

    // void *operator new(size_t size) {
    //   void *ret;
    //   alloc_memalign(&ret,64,size);
    //   return ret;
    // }

    uint32_t GetLevel() {
      return hdr.level;
    }
    
    void linear_search_range(ram_entry_key_t min, ram_entry_key_t max, std::vector<std::string> &values, int &size);
    inline int count() {
      uint8_t previous_switch_counter;
      int count = 0;
      do {
        previous_switch_counter = hdr.switch_counter;
        count = hdr.last_index + 1;

        while(count >= 0 && records[count].ptr != NULL) {
          if(RAM_IS_FORWARD(previous_switch_counter))
            ++count;
          else
            --count;
        } 

        if(count < 0) {
          count = 0;
          while(records[count].ptr != NULL) {
            ++count;
          }
        }

      } while(previous_switch_counter != hdr.switch_counter);

      return count;
    }

    inline bool remove_key(ram_entry_key_t key) {
      // Set the switch_counter
      if(RAM_IS_FORWARD(hdr.switch_counter)) 
        ++hdr.switch_counter;

      bool shift = false;
      int i;
      for(i = 0; records[i].ptr != NULL; ++i) {
        if(!shift && records[i].key == key) {
          records[i].ptr = (i == 0) ? 
            (char *)hdr.leftmost_ptr : records[i - 1].ptr; 
          shift = true;
        }

        if(shift) {
          records[i].key = records[i + 1].key;
          records[i].ptr = records[i + 1].ptr;
        }
      }

      if(shift) {
        --hdr.last_index;
      }
      return shift;
    }

    bool remove(ram_tree* bt, ram_entry_key_t key, bool only_rebalance = false, bool with_lock = true) {
      hdr.mtx->lock();

      bool ret = remove_key(key);

      hdr.mtx->unlock();

      return ret;
    }

    /*
     * Although we implemented the rebalancing of B+-Tree, it is currently blocked for the performance.
     * Please refer to the follow.
     * Chi, P., Lee, W. C., & Xie, Y. (2014, August). 
     * Making B+-tree efficient in PCM-based main memory. In Proceedings of the 2014
     * international symposium on Low power electronics and design (pp. 69-74). ACM.
     */
    bool remove_rebalancing(ram_tree* bt, ram_entry_key_t key, bool only_rebalance = false, bool with_lock = true) {
      if(with_lock) {
        hdr.mtx->lock();
      }
      if(hdr.is_deleted) {
        if(with_lock) {
          hdr.mtx->unlock();
        }
        return false;
      }

      if(!only_rebalance) {
        register int num_entries_before = count();

        // This node is root
        if(this == (ram_node *)bt->root) {
          if(hdr.level > 0) {
            if(num_entries_before == 1 && !hdr.sibling_ptr) {
              bt->root = (char *)hdr.leftmost_ptr;

              hdr.is_deleted = 1;
            }
          }

          // Remove the key from this node
          bool ret = remove_key(key);

          if(with_lock) {
            hdr.mtx->unlock();
          }
          return true;
        }

        bool should_rebalance = true;
        // check the node utilization
        if(num_entries_before - 1 >= (int)((ram_cardinality - 1) * 0.5)) { 
          should_rebalance = false;
        }

        // Remove the key from this node
        bool ret = remove_key(key);

        if(!should_rebalance) {
          if(with_lock) {
            hdr.mtx->unlock();
          }
          return (hdr.leftmost_ptr == NULL) ? ret : true;
        }
      } 

      //Remove a key from the parent node
      ram_entry_key_t deleted_key_from_parent = 0;
      bool is_leftmost_node = false;
      ram_node *left_sibling;
      bt->btree_delete_internal(key, (char *)this, hdr.level + 1,
          &deleted_key_from_parent, &is_leftmost_node, &left_sibling);

      if(is_leftmost_node) {
        if(with_lock) {
          hdr.mtx->unlock();
        }

        if(!with_lock) {
          hdr.sibling_ptr->hdr.mtx->lock();
        }
        hdr.sibling_ptr->remove(bt, hdr.sibling_ptr->records[0].key, true, with_lock);
        if(!with_lock) {
          hdr.sibling_ptr->hdr.mtx->unlock();
        }
        return true;
      }

      if(with_lock) {
        left_sibling->hdr.mtx->lock();
      }

      while(left_sibling->hdr.sibling_ptr != this) {
        if(with_lock) {
          ram_node *t = left_sibling->hdr.sibling_ptr;
          left_sibling->hdr.mtx->unlock();
          left_sibling = t;
          left_sibling->hdr.mtx->lock();
        }
        else
          left_sibling = left_sibling->hdr.sibling_ptr;
      }

      register int num_entries = count();
      register int left_num_entries = left_sibling->count();

      // Merge or Redistribution
      int total_num_entries = num_entries + left_num_entries;
      if(hdr.leftmost_ptr)
        ++total_num_entries;

      ram_entry_key_t parent_key;

      if(total_num_entries > ram_cardinality - 1) { // Redistribution
        register int m = (int) ceil(total_num_entries / 2);

        if(num_entries < left_num_entries) { // left -> right
          if(hdr.leftmost_ptr == nullptr){
            for(int i=left_num_entries - 1; i>=m; i--){
              insert_key
                (left_sibling->records[i].key, left_sibling->records[i].ptr, &num_entries); 
            } 

            left_sibling->records[m].ptr = nullptr;

            left_sibling->hdr.last_index = m - 1;

            parent_key = records[0].key; 
          }
          else{
            insert_key(deleted_key_from_parent, (char*)hdr.leftmost_ptr,
                &num_entries); 

            for(int i=left_num_entries - 1; i>m; i--){
              insert_key
                (left_sibling->records[i].key, left_sibling->records[i].ptr, &num_entries); 
            }

            parent_key = left_sibling->records[m].key; 

            hdr.leftmost_ptr = (ram_node*)left_sibling->records[m].ptr; 
            // clflush((char *)&(hdr.leftmost_ptr), sizeof(ram_node *));

            left_sibling->records[m].ptr = nullptr;
            // clflush((char *)&(left_sibling->records[m].ptr), sizeof(char *));

            left_sibling->hdr.last_index = m - 1;
            // clflush((char *)&(left_sibling->hdr.last_index), sizeof(int16_t));
          }

          if(left_sibling == ((ram_node *)bt->root)) {
            ram_node* new_root = new (bt->Newram_node()) ram_node(left_sibling, parent_key, this, hdr.level + 1);
            bt->setNewRoot((char *)new_root);
          }
          else {
            bt->btree_insert_internal
              ((char *)left_sibling, parent_key, (char *)this, hdr.level + 1);
          }
        }
        else{ // from leftmost case
          hdr.is_deleted = 1;
          // clflush((char *)&(hdr.is_deleted), sizeof(uint8_t));

          ram_node* new_sibling = new (bt->Newram_node()) ram_node(hdr.level); 
          new_sibling->hdr.mtx->lock();
          new_sibling->hdr.sibling_ptr = hdr.sibling_ptr;

          int num_dist_entries = num_entries - m;
          int new_sibling_cnt = 0;

          if(hdr.leftmost_ptr == nullptr){
            for(int i=0; i<num_dist_entries; i++){
              left_sibling->insert_key(records[i].key, records[i].ptr,
                  &left_num_entries); 
            } 

            for(int i=num_dist_entries; records[i].ptr != NULL; i++){
              new_sibling->insert_key(records[i].key, records[i].ptr,
                  &new_sibling_cnt, false); 
            } 

            // clflush((char *)(new_sibling), sizeof(ram_node));

            left_sibling->hdr.sibling_ptr = new_sibling;
            // clflush((char *)&(left_sibling->hdr.sibling_ptr), sizeof(ram_node *));

            parent_key = new_sibling->records[0].key; 
          }
          else{
            left_sibling->insert_key(deleted_key_from_parent,
                (char*)hdr.leftmost_ptr, &left_num_entries);

            for(int i=0; i<num_dist_entries - 1; i++){
              left_sibling->insert_key(records[i].key, records[i].ptr,
                  &left_num_entries); 
            } 

            parent_key = records[num_dist_entries - 1].key;

            new_sibling->hdr.leftmost_ptr = (ram_node*)records[num_dist_entries - 1].ptr;
            for(int i=num_dist_entries; records[i].ptr != NULL; i++){
              new_sibling->insert_key(records[i].key, records[i].ptr,
                  &new_sibling_cnt, false); 
            } 
            // clflush((char *)(new_sibling), sizeof(ram_node));

            left_sibling->hdr.sibling_ptr = new_sibling;
            // clflush((char *)&(left_sibling->hdr.sibling_ptr), sizeof(ram_node *));
          }

          if(left_sibling == ((ram_node *)bt->root)) {
            ram_node* new_root = new (bt->Newram_node()) ram_node(left_sibling, parent_key, new_sibling, hdr.level + 1);
            bt->setNewRoot((char *)new_root);
          }
          else {
            bt->btree_insert_internal
              ((char *)left_sibling, parent_key, (char *)new_sibling, hdr.level + 1);
          }

          new_sibling->hdr.mtx->unlock();
        }
      }
      else {
        hdr.is_deleted = 1;
        // clflush((char *)&(hdr.is_deleted), sizeof(uint8_t));

        if(hdr.leftmost_ptr)
          left_sibling->insert_key(deleted_key_from_parent, 
              (char *)hdr.leftmost_ptr, &left_num_entries);

        for(int i = 0; records[i].ptr != NULL; ++i) { 
          left_sibling->insert_key(records[i].key, records[i].ptr, &left_num_entries);
        }

        left_sibling->hdr.sibling_ptr = hdr.sibling_ptr;
        // clflush((char *)&(left_sibling->hdr.sibling_ptr), sizeof(ram_node *));
      }

      if(with_lock) {
        left_sibling->hdr.mtx->unlock();
        hdr.mtx->unlock();
      }

      return true;
    }

    inline void 
      insert_key(ram_entry_key_t key, char* ptr, int *num_entries, bool flush = true,
          bool update_last_index = true) {
        // update switch_counter
        if(!RAM_IS_FORWARD(hdr.switch_counter))
          ++hdr.switch_counter;

        // FAST
        if(*num_entries == 0) {  // this ram_node is empty
          ram_entry* new_entry = (ram_entry*) &records[0];
          ram_entry* array_end = (ram_entry*) &records[1];
          new_entry->key = (ram_entry_key_t) key;
          new_entry->ptr = (char*) ptr;

          array_end->ptr = (char*)NULL;
        }
        else {
          int i = *num_entries - 1, inserted = 0, to_flush_cnt = 0;
          records[*num_entries+1].ptr = records[*num_entries].ptr; 

          // FAST
          for(i = *num_entries - 1; i >= 0; i--) {
            if(key < records[i].key ) {
              records[i+1].ptr = records[i].ptr;
              records[i+1].key = records[i].key;
            }
            else{
              records[i+1].ptr = records[i].ptr;
              records[i+1].key = key;
              records[i+1].ptr = ptr;

              // if(flush)
                // clflush((char*)&records[i+1],sizeof(ram_entry));
              inserted = 1;
              break;
            }
          }
          if(inserted==0){
            records[0].ptr =(char*) hdr.leftmost_ptr;
            records[0].key = key;
            records[0].ptr = ptr;
            // if(flush)
              // clflush((char*) &records[0], sizeof(ram_entry)); 
          }
        }

        if(update_last_index) {
          hdr.last_index = *num_entries;
          // clflush((char *)&(hdr.last_index), sizeof(int16_t));
        }
        ++(*num_entries);
      }

    // Insert a new key - FAST and FAIR
    ram_node *store
      (ram_tree* bt, char* left, ram_entry_key_t key, char* right,
       bool flush, bool with_lock, ram_node *invalid_sibling = NULL) {
        if(with_lock) {
          hdr.mtx->lock(); // Lock the write lock
        }
        if(hdr.is_deleted) {
          if(with_lock) {
            hdr.mtx->unlock();
          }

          return NULL;
        }

        // If this node has a sibling node,
        if(hdr.sibling_ptr && (hdr.sibling_ptr != invalid_sibling)) {
          // Compare this key with the first key of the sibling
          if(key > hdr.sibling_ptr->records[0].key) {
            if(with_lock) { 
              hdr.mtx->unlock(); // Unlock the write lock
            }
            return hdr.sibling_ptr->store(bt, NULL, key, right, 
                true, with_lock, invalid_sibling);
          }
        }

        register int num_entries = count();

        // FAST
        if(num_entries < ram_cardinality - 1) {
          insert_key(key, right, &num_entries, flush);

          if(with_lock) {
            hdr.mtx->unlock(); // Unlock the write lock
          }

          return this;
        }
        else {// FAIR
          // overflow
          // create a new node
          ram_node* sibling = new (bt->Newram_node()) ram_node(hdr.level); 
          register int m = (int) ceil(num_entries/2);
          ram_entry_key_t split_key = records[m].key;

          // migrate half of keys into the sibling
          int sibling_cnt = 0;
          if(hdr.leftmost_ptr == NULL){ // leaf node
            for(int i=m; i<num_entries; ++i){ 
              sibling->insert_key(records[i].key, records[i].ptr, &sibling_cnt, false);
            }
          }
          else{ // internal node
            for(int i=m+1;i<num_entries;++i){ 
              sibling->insert_key(records[i].key, records[i].ptr, &sibling_cnt, false);
            }
            sibling->hdr.leftmost_ptr = (ram_node*) records[m].ptr;
          }

          sibling->hdr.sibling_ptr = hdr.sibling_ptr;
          // clflush((char *)sibling, sizeof(ram_node));

          hdr.sibling_ptr = sibling;
          // clflush((char*) &hdr, sizeof(hdr));

          // set to NULL
          if(RAM_IS_FORWARD(hdr.switch_counter))
            hdr.switch_counter += 2;
          else
            ++hdr.switch_counter;
          records[m].ptr = NULL;
          // clflush((char*) &records[m], sizeof(ram_entry));

          hdr.last_index = m - 1;
          // clflush((char *)&(hdr.last_index), sizeof(int16_t));

          num_entries = hdr.last_index + 1;

          ram_node *ret;

          // insert the key
          if(key < split_key) {
            insert_key(key, right, &num_entries);
            ret = this;
          }
          else {
            sibling->insert_key(key, right, &sibling_cnt);
            ret = sibling;
          }

          // Set a new root or insert the split key to the parent
          if(bt->root == (char *)this) { // only one node can update the root ptr
            ram_node* new_root = new (bt->Newram_node()) ram_node((ram_node*)this, split_key, sibling, 
                hdr.level + 1);
            bt->setNewRoot((char *)new_root);

            if(with_lock) {
              hdr.mtx->unlock(); // Unlock the write lock
            }
          }
          else {
            if(with_lock) {
              hdr.mtx->unlock(); // Unlock the write lock
            }
            bt->btree_insert_internal(NULL, split_key, (char *)sibling, 
                hdr.level + 1);
          }

          return ret;
        }

      }

    // Search keys with linear search
    void linear_search_range
      (ram_entry_key_t min, ram_entry_key_t max, unsigned long *buf) {
        int i, off = 0;
        uint8_t previous_switch_counter;
        ram_node *current = this;

        while(current) {
          int old_off = off;
          do {
            previous_switch_counter = current->hdr.switch_counter;
            off = old_off;

            ram_entry_key_t tmp_key;
            char *tmp_ptr;

            if(RAM_IS_FORWARD(previous_switch_counter)) {
              if((tmp_key = current->records[0].key) > min) {
                if(tmp_key < max) {
                  if((tmp_ptr = current->records[0].ptr) != NULL) {
                    if(tmp_key == current->records[0].key) {
                      if(tmp_ptr) {
                        buf[off++] = (unsigned long)tmp_ptr;
                      }
                    }
                  }
                }
                else
                  return;
              }

              for(i=1; current->records[i].ptr != NULL; ++i) { 
                if((tmp_key = current->records[i].key) > min) {
                  if(tmp_key < max) {
                    if((tmp_ptr = current->records[i].ptr) != current->records[i - 1].ptr) {
                      if(tmp_key == current->records[i].key) {
                        if(tmp_ptr)
                          buf[off++] = (unsigned long)tmp_ptr;
                      }
                    }
                  }
                  else
                    return;
                }
              }
            }
            else {
              for(i=count() - 1; i > 0; --i) { 
                if((tmp_key = current->records[i].key) > min) {
                  if(tmp_key < max) {
                    if((tmp_ptr = current->records[i].ptr) != current->records[i - 1].ptr) {
                      if(tmp_key == current->records[i].key) {
                        if(tmp_ptr)
                          buf[off++] = (unsigned long)tmp_ptr;
                      }
                    }
                  }
                  else
                    return;
                }
              }

              if((tmp_key = current->records[0].key) > min) {
                if(tmp_key < max) {
                  if((tmp_ptr = current->records[0].ptr) != NULL) {
                    if(tmp_key == current->records[0].key) {
                      if(tmp_ptr) {
                        buf[off++] = (unsigned long)tmp_ptr;
                      }
                    }
                  }
                }
                else
                  return;
              }
            }
          } while(previous_switch_counter != current->hdr.switch_counter);

          current = current->hdr.sibling_ptr;
        }
      }

    char *linear_search(ram_entry_key_t key) {
      int i = 1;
      uint8_t previous_switch_counter;
      char *ret = NULL;
      char *t; 
      ram_entry_key_t k;
      ram_entry *find_entry = NULL;

      if(hdr.leftmost_ptr == NULL) { // Search a leaf node
        do {
          previous_switch_counter = hdr.switch_counter;
          ret = NULL;

          // search from left ro right
          if(RAM_IS_FORWARD(previous_switch_counter)) { 
            if((k = records[0].key) == key) {
              if((t = records[0].ptr) != NULL) {
                if(k == records[0].key) {
                  ret = t;
                  find_entry = &records[0];
                  continue;
                }
              }
            }

            for(i=1; records[i].ptr != NULL; ++i) { 
              if((k = records[i].key) == key) {
                if(records[i-1].ptr != (t = records[i].ptr)) {
                  if(k == records[i].key) {
                    find_entry = &records[i];
                    ret = t;
                    break;
                  }
                }
              }
            }
          }
          else { // search from right to left
            for(i = count() - 1; i > 0; --i) {
              if((k = records[i].key) == key) {
                if(records[i - 1].ptr != (t = records[i].ptr) && t) {
                  if(k == records[i].key) {
                    find_entry = &records[i];
                    ret = t;
                    break;
                  }
                }
              }
            }

            if(!ret) {
              if((k = records[0].key) == key) {
                if(NULL != (t = records[0].ptr) && t) {
                  if(k == records[0].key) {
                    find_entry = &records[0];
                    ret = t;
                    continue;
                  }
                }
              }
            }
          }
        } while(hdr.switch_counter != previous_switch_counter);

        if(find_entry) find_entry->key.hot++;
        if(ret) {
          return ret;
        }

        if((t = (char *)hdr.sibling_ptr) && key >= ((ram_node *)t)->records[0].key)
          return t;

        return NULL;
      }
      else { // internal node
        do {
          previous_switch_counter = hdr.switch_counter;
          ret = NULL;

          if(RAM_IS_FORWARD(previous_switch_counter)) {
            if(key < (k = records[0].key)) {
              if((t = (char *)hdr.leftmost_ptr) != records[0].ptr) { 
                ret = t;
                continue;
              }
            }

            for(i = 1; records[i].ptr != NULL; ++i) { 
              if(key < (k = records[i].key)) { 
                if((t = records[i-1].ptr) != records[i].ptr) {
                  ret = t;
                  break;
                }
              }
            }

            if(!ret) {
              ret = records[i - 1].ptr; 
              continue;
            }
          }
          else { // search from right to left
            for(i = count() - 1; i >= 0; --i) {
              if(key >= (k = records[i].key)) {
                if(i == 0) {
                  if((char *)hdr.leftmost_ptr != (t = records[i].ptr)) {
                    ret = t;
                    break;
                  }
                }
                else {
                  if(records[i - 1].ptr != (t = records[i].ptr)) {
                    ret = t;
                    break;
                  }
                }
              }
            }
          }
        } while(hdr.switch_counter != previous_switch_counter);

        if((t = (char *)hdr.sibling_ptr) != NULL) {
          if(key >= ((ram_node *)t)->records[0].key)
            return t;
        }

        if(ret) {
          return ret;
        }
        else
          return (char *)hdr.leftmost_ptr;
      }

      return NULL;
    }

    // print a node 
    void print() {
      if(hdr.leftmost_ptr == NULL) 
        printf("[%d] leaf %x \n", this->hdr.level, this);
      else 
        printf("[%d] internal %x \n", this->hdr.level, this);
      printf("last_index: %d\n", hdr.last_index);
      printf("switch_counter: %d\n", hdr.switch_counter);
      printf("search direction: ");
      if(RAM_IS_FORWARD(hdr.switch_counter))
        printf("->\n");
      else
        printf("<-\n");

      if(hdr.leftmost_ptr!=NULL) 
        printf("%x ",hdr.leftmost_ptr);

      for(int i=0;records[i].ptr != NULL;++i)
        printf("%ld,%x ",records[i].key,records[i].ptr);

      printf("%x ", hdr.sibling_ptr);

      printf("\n");
    }

    void printAll() {
      if(hdr.leftmost_ptr==NULL) {
        printf("printing leaf node: ");
        print();
      }
      else {
        printf("printing internal node: ");
        print();
        ((ram_node*) hdr.leftmost_ptr)->printAll();
        for(int i=0;records[i].ptr != NULL;++i){
          ((ram_node*) records[i].ptr)->printAll();
        }
      }
    }

    void CalculateSapce(uint64_t &space) {
      if(hdr.leftmost_ptr==NULL) {
        space += RAM_PAGESIZE;
      }
      else {
        space += RAM_PAGESIZE;
        ((ram_node*) hdr.leftmost_ptr)->CalculateSapce(space);
        for(int i=0;records[i].ptr != NULL;++i){
          ((ram_node*) records[i].ptr)->CalculateSapce(space);
        }
      }
    }
};