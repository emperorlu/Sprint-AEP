
#include "ram_btree.h"

pthread_mutex_t print_mtx;


/*
 * class ram_tree
 */

void ram_node::linear_search_range(ram_entry_key_t min, ram_entry_key_t max, std::vector<std::string> &values, int &size) {
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




void* ram_tree::Newram_node() {
    return new ram_node();
}

void ram_tree::btree_init() {
      // node_alloc = new NVMAllocator(path, keysize);
      // if(node_alloc == nullptr) {
      //     exit(0);
      // }
      root = (char*)(new (Newram_node()) ram_node());
}

// ram_tree::ram_tree(ram_node *root_) {
//     // if()
//     if(root_ == nullptr) {
//         root = (char*)new ram_node();
//         height = 1;
//     } else {
//         root = (char *)root_;
//         height = root_->GetLevel() + 1;
//     }
//     // node_alloc = nullptr;
//     print_log(LV_DEBUG, "root is %p, ram_tree is %p, height is %d", root, this, height);
// }

void ram_tree::setNewRoot(char *new_root) {
  this->root = (char*)new_root;
  // clflush((char*)&(this->root),sizeof(char*));
  ++height;
}

char *ram_tree::btree_search(ram_entry_key_t key){
  ram_node* p = (ram_node*)root;

  while(p->hdr.leftmost_ptr != NULL) {
    p = (ram_node *)p->linear_search(key);
  }

  ram_node *t;
  while((t = (ram_node *)p->linear_search(key)) == p->hdr.sibling_ptr) {
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
void ram_tree::btree_insert(ram_entry_key_t key, char* right){ //need to be string
  ram_node* p = (ram_node*)root;

  while(p->hdr.leftmost_ptr != NULL) {
    p = (ram_node*)p->linear_search(key);
  }

  if(!p->store(this, NULL, key, right, true, true)) { // store 
    btree_insert(key, right);
  }
}

// store the key into the node at the given level 
void ram_tree::btree_insert_internal
(char *left, ram_entry_key_t key, char *right, uint32_t level) {
  if(level > ((ram_node *)root)->hdr.level)
    return;

  ram_node *p = (ram_node *)this->root;

  while(p->hdr.level > level) 
    p = (ram_node *)p->linear_search(key);

  if(!p->store(this, NULL, key, right, true, true)) {
    btree_insert_internal(left, key, right, level);
  }
}

void ram_tree::btree_delete(ram_entry_key_t key) {
  ram_node* p = (ram_node*)root;

  while(p->hdr.leftmost_ptr != NULL){
    p = (ram_node*) p->linear_search(key);
  }

  ram_node *t;
  while((t = (ram_node *)p->linear_search(key)) == p->hdr.sibling_ptr) {
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

void ram_tree::btree_delete_internal(ram_entry_key_t key, char *ptr, uint32_t level, ram_entry_key_t *deleted_key, 
 bool *is_leftmost_node, ram_node **left_sibling) {
  if(level > ((ram_node *)this->root)->hdr.level)
    return;

  ram_node *p = (ram_node *)this->root;

  while(p->hdr.level > level) {
    p = (ram_node *)p->linear_search(key);
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
          *left_sibling = (ram_node *)p->records[i - 1].ptr;
          p->remove(this, *deleted_key, false, false);
          break;
        }
      }
    }
  }

  p->hdr.mtx->unlock();
}

// Function to search keys from "min" to "max"
void ram_tree::btree_search_range(ram_entry_key_t min, ram_entry_key_t max, unsigned long *buf) {
  ram_node *p = (ram_node *)root;

  while(p) {
    if(p->hdr.leftmost_ptr != NULL) {
      // The current ram_node is internal
      p = (ram_node *)p->linear_search(min);
    }
    else {
      // Found a leaf
      p->linear_search_range(min, max, buf);

      break;
    }
  }
}

void ram_tree::btree_search_range(ram_entry_key_t min, ram_entry_key_t max, 
        std::vector<std::string> &values, int &size) {
    ram_node *p = (ram_node *)root;

    while(p) {
        if(p->hdr.leftmost_ptr != NULL) {
        // The current ram_node is internal
        p = (ram_node *)p->linear_search(min);
        }
        else {
        // Found a leaf
        p->linear_search_range(min, max, values, size);

        break;
        }
    }
}

void ram_tree::printAll(){
  pthread_mutex_lock(&print_mtx);
  int total_keys = 0;
  ram_node *leftmost = (ram_node *)root;
  printf("root: %x\n", root);
  do {
    ram_node *sibling = leftmost;
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

// void ram_tree::for_each() {
//   ram_node *node = (ram_node *)root;
//   while(node->hdr.leftmost_ptr) {
//     node = node->hdr.leftmost_ptr;
//   }
//   while(node) {
//     // hhjbkjkjj
//     node = node->hdr.sibling_ptr;
//   }
// }


void ram_tree::CalculateSapce(uint64_t &space) {
    if(root != nullptr) {
        ((ram_node*)root)->CalculateSapce(space);
    }
}


void ram_tree::PrintInfo() {
    printf("This is a b+ tree.\n");
    printf("Node size is %lu, M path is %d.\n", sizeof(ram_node), ram_cardinality);
    printf("Tree height is %d.\n", height);
}