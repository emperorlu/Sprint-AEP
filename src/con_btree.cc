
#include "con_btree.h"

pthread_mutex_t print_mtx;


/*
 * class btree
 */

void bpnode::linear_search_range(entry_key_t min, entry_key_t max, std::vector<std::string> &values, int &size) {
    int i, off = 0;
    uint8_t previous_switch_counter;
    bpnode *current = this;

    while(current) {
        int old_off = off;
        do {
            previous_switch_counter = current->hdr.switch_counter;
            off = old_off;

            entry_key_t tmp_key;
            char *tmp_ptr;

            if(IS_FORWARD(previous_switch_counter)) {
                if((tmp_key = current->records[0].key) > min) {
                    if(tmp_key < max) {
                        if((tmp_ptr = current->records[0].ptr) != NULL) {
                            if(tmp_key == current->records[0].key) {
                                if(tmp_ptr) {
                                    // buf[off++] = (unsigned long)tmp_ptr;
                                    values.push_back(string(tmp_ptr, NVM_ValueSize));
                                    off++;
                                    if(off >= size) {
                                        return ;
                                    }
                                }
                            }
                        }
                    }
                    else {
                        size = off;
                        return;
                    }
                }

                for(i=1; current->records[i].ptr != NULL; ++i) { 
                    if((tmp_key = current->records[i].key) > min) {
                        if(tmp_key < max) {
                            if((tmp_ptr = current->records[i].ptr) != current->records[i - 1].ptr) {
                                if(tmp_key == current->records[i].key) {
                                    if(tmp_ptr) {
                                        // buf[off++] = (unsigned long)tmp_ptr;
                                        values.push_back(string(tmp_ptr, NVM_ValueSize));
                                        off++;
                                        if(off >= size) {
                                            return ;
                                        }
                                    }
                                }
                            }
                        }
                        else {
                            size = off;
                            return;
                        }
                    }
                }
            }
            else {
                for(i=count() - 1; i > 0; --i) { 
                    if((tmp_key = current->records[i].key) > min) {
                        if(tmp_key < max) {
                            if((tmp_ptr = current->records[i].ptr) != current->records[i - 1].ptr) {
                                if(tmp_key == current->records[i].key) {
                                    if(tmp_ptr) {
                                        // buf[off++] = (unsigned long)tmp_ptr;
                                        values.push_back(string(tmp_ptr, NVM_ValueSize));
                                        off++;
                                        if(off >= size) {
                                            return ;
                                        }
                                    }
                                }
                            }
                        }
                        else {
                            size = off;
                            return;
                        }
                    }
                }

                if((tmp_key = current->records[0].key) > min) {
                    if(tmp_key < max) {
                        if((tmp_ptr = current->records[0].ptr) != NULL) {
                            if(tmp_key == current->records[0].key) {
                                if(tmp_ptr) {
                                    // buf[off++] = (unsigned long)tmp_ptr;
                                    values.push_back(string(tmp_ptr, NVM_ValueSize));
                                    off++;
                                    if(off >= size) {
                                        return ;
                                    }
                                }
                            }
                        }
                    }
                    else {
                        size = off;
                        return;
                    }
                }
            }
        } while(previous_switch_counter != current->hdr.switch_counter);

        current = current->hdr.sibling_ptr;
    }
    size = off;
}

btree::btree(){
  // root = (char*)new bpnode();
  HCrchain = new CONRangChain;
  // Cache = new CashTable(10000000);
  height = 1;
  node_alloc = nullptr;
}

// void btree::chain_insert(entry_key_t key){
//   HCrchain->insert(key);
// }

void btree::btree_updakey(entry_key_t key){
  // Cache->insert(key);
  HCrchain->insert(key);
}

static long bcak_count = 0;
vector<entry_key_t> btree::btree_back(int hot, size_t read){
  vector<entry_key_t> dlist;
  bcak_count++;
  // cout << "begin " << bcak_count << " back" << endl; 
  for(int i = HCrchain->theLists.size()-1; i >= 0; i--)
  {
    typename list<entry_key_t>::iterator itr = HCrchain->theLists[i].begin();
    while(itr != HCrchain->theLists[i].end()){
      if((*itr).hot < hot){
        return dlist;
      }
      dlist.push_back((*itr));
      itr = HCrchain->theLists[i].erase(itr);
      if (dlist.size() >= read)
        return dlist; 
    }
  }
  return dlist;
}



void* btree::NewBpNode() {
    return node_alloc->Allocate(sizeof(bpnode));
}

void btree::btree_init(const std::string &path, uint64_t keysize) {
      node_alloc = new NVMAllocator(path, keysize);
      if(node_alloc == nullptr) {
          exit(0);
      }
      root = (char*)(new (NewBpNode()) bpnode());
}

// btree::btree(bpnode *root_) {
//     // if()
//     if(root_ == nullptr) {
//         root = (char*)new bpnode();
//         height = 1;
//     } else {
//         root = (char *)root_;
//         height = root_->GetLevel() + 1;
//     }
//     // node_alloc = nullptr;
//     print_log(LV_DEBUG, "root is %p, btree is %p, height is %d", root, this, height);
// }

void btree::setNewRoot(char *new_root) {
  this->root = (char*)new_root;
  clflush((char*)&(this->root),sizeof(char*));
  ++height;
}

char *btree::btree_search(entry_key_t key){
  bpnode* p = (bpnode*)root;

  while(p->hdr.leftmost_ptr != NULL) {
    p = (bpnode *)p->linear_search(key);
  }

  bpnode *t;
  while((t = (bpnode *)p->linear_search(key)) == p->hdr.sibling_ptr) {
    p = t;
    if(!p) {
      break;
    }
  }

  if(!t) {
    // printf("NOT FOUND %lu, t = %x\n", key, t);
    return NULL;
  }

  return (char *)t;
}

// insert the key in the leaf node
void btree::btree_insert(entry_key_t key, char* right){ //need to be string
  bpnode* p = (bpnode*)root;

  while(p->hdr.leftmost_ptr != NULL) {
    p = (bpnode*)p->linear_search(key);
  }

  if(!p->store(this, NULL, key, right, true, true)) { // store 
    btree_insert(key, right);
  }
}

// store the key into the node at the given level 
void btree::btree_insert_internal
(char *left, entry_key_t key, char *right, uint32_t level) {
  if(level > ((bpnode *)root)->hdr.level)
    return;

  bpnode *p = (bpnode *)this->root;

  while(p->hdr.level > level) 
    p = (bpnode *)p->linear_search(key);

  if(!p->store(this, NULL, key, right, true, true)) {
    btree_insert_internal(left, key, right, level);
  }
}

void btree::btree_delete(entry_key_t key) {
  bpnode* p = (bpnode*)root;

  while(p->hdr.leftmost_ptr != NULL){
    p = (bpnode*) p->linear_search(key);
  }

  bpnode *t;
  while((t = (bpnode *)p->linear_search(key)) == p->hdr.sibling_ptr) {
    p = t;
    if(!p)
      break;
  }

  if(p && t) {
    if(!p->remove(this, key)) {
      btree_delete(key);
    }
  }
  else {
      ;
    // printf("not found the key to delete %lu\n", key);
  }
}

void btree::btree_delete_internal(entry_key_t key, char *ptr, uint32_t level, entry_key_t *deleted_key, 
 bool *is_leftmost_node, bpnode **left_sibling) {
  if(level > ((bpnode *)this->root)->hdr.level)
    return;

  bpnode *p = (bpnode *)this->root;

  while(p->hdr.level > level) {
    p = (bpnode *)p->linear_search(key);
  }

  p->hdr.mtx->lock();

  if((char *)p->hdr.leftmost_ptr == ptr) {
    *is_leftmost_node = true;
    p->hdr.mtx->unlock();
    return;
  }

  *is_leftmost_node = false;

  for(int i=0; p->records[i].ptr != NULL; ++i) {
    if(p->records[i].ptr == ptr) {
      if(i == 0) {
        if((char *)p->hdr.leftmost_ptr != p->records[i].ptr) {
          *deleted_key = p->records[i].key;
          *left_sibling = p->hdr.leftmost_ptr;
          p->remove(this, *deleted_key, false, false);
          break;
        }
      }
      else {
        if(p->records[i - 1].ptr != p->records[i].ptr) {
          *deleted_key = p->records[i].key;
          *left_sibling = (bpnode *)p->records[i - 1].ptr;
          p->remove(this, *deleted_key, false, false);
          break;
        }
      }
    }
  }

  p->hdr.mtx->unlock();
}

// Function to search keys from "min" to "max"
void btree::btree_search_range(entry_key_t min, entry_key_t max, unsigned long *buf) {
  bpnode *p = (bpnode *)root;

  while(p) {
    if(p->hdr.leftmost_ptr != NULL) {
      // The current bpnode is internal
      p = (bpnode *)p->linear_search(min);
    }
    else {
      // Found a leaf
      p->linear_search_range(min, max, buf);

      break;
    }
  }
}

void btree::btree_search_range(entry_key_t min, entry_key_t max, 
        std::vector<std::string> &values, int &size) {
    bpnode *p = (bpnode *)root;

    while(p) {
        if(p->hdr.leftmost_ptr != NULL) {
        // The current bpnode is internal
        p = (bpnode *)p->linear_search(min);
        }
        else {
        // Found a leaf
        p->linear_search_range(min, max, values, size);

        break;
        }
    }
}

void btree::printAll(){
  pthread_mutex_lock(&print_mtx);
  int total_keys = 0;
  bpnode *leftmost = (bpnode *)root;
  printf("root: %x\n", root);
  do {
    bpnode *sibling = leftmost;
    while(sibling) {
      if(sibling->hdr.level == 0) {
        total_keys += sibling->hdr.last_index + 1;
      }
      sibling->print();
      sibling = sibling->hdr.sibling_ptr;
    }
    printf("-----------------------------------------\n");
    leftmost = leftmost->hdr.leftmost_ptr;
  } while(leftmost);

  printf("total number of keys: %d\n", total_keys);
  pthread_mutex_unlock(&print_mtx);
}

// void btree::for_each() {
//   bpnode *node = (bpnode *)root;
//   while(node->hdr.leftmost_ptr) {
//     node = node->hdr.leftmost_ptr;
//   }
//   while(node) {
//     // hhjbkjkjj
//     node = node->hdr.sibling_ptr;
//   }
// }


void btree::CalculateSapce(uint64_t &space) {
    if(root != nullptr) {
        ((bpnode*)root)->CalculateSapce(space);
    }
}


void btree::PrintInfo() {
    printf("This is a b+ tree.\n");
    printf("Node size is %lu, M path is %d.\n", sizeof(bpnode), cardinality);
    printf("Tree height is %d.\n", height);
}