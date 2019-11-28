CXX = g++
LDFLAGS = -pthread -lpmemobj -lpmem #-lcityhash
DEFS = -DCZL_DEBUG -DEVERY_1G_PRINT -DDRAM_CANCEL_LOG -DUSE_MUIL_THREAD# -DCAL_ACCESS_COUNT
IFLAGS = -I../../system
CXXFLAGS += $(DEFS) $(IFLAGS)
AR = ar
ARFLAGS = rs
# con_btree.cc nvm_btree.cc
# persistent_BPlusTree_Wrapper.cc
# nvmbptree.cc
#			con_btree.cc
LIB_SOURCES = persistent_skiplist_no_transaction.cc\
			dram_nvmbptree.cc\
			drnvm_BPlusTree_Wrapper.cc\
			nvm_btree.cc\
			ram_btree.cc\
			dram_btree.cc\
			con_btree.cc\
			statistic.cc\
			aep_system_thread.cc\
			nvm_allocator.cc

TEST_TARGET = system\

LIBOBJECTS = $(LIB_SOURCES:.cc=.o)

TARGET_OBJS = $(LIB_SOURCES:.cc=)

LIBRARY = my_skip_list.a

.PHONY: clean
default:  all

all:  clean test

clean: 
	rm -f $(LIBRARY)
	rm -f $(LIBOBJECTS)
	rm -f $(TEST_TARGET)
	rm -f test

$(LIBOBJECTS): 
	for sou_file in $(TARGET_OBJS) ; do \
	$(CXX) $(CXXFLAGS) $(LDFLAGS) -c $$sou_file.cc -o $$sou_file.o; \
	done;

$(LIBRARY) : $(LIBOBJECTS)
	rm -f $@
	$(AR) $(ARFLAGS) $@ $^

$(TEST_TARGET) :
	for test_file in $(TEST_TARGET) ; do \
	$(CXX) $(CXXFLAGS) $$test_file.cc -o $$test_file $(LIBRARY) $(LDFLAGS); \
	done;

test:
	$(CXX) $(CXXFLAGS) system.cc $(LIB_SOURCES) -o test $(LDFLAGS);
	#$(CXX) $(CXXFLAGS) test_pmem_memcpy.cc $(LIB_SOURCES) -o test_pmem $(LDFLAGS);
#$(CXX) $(CXXFLAGS) batch_test_new.cc random.cc -o test $(LDFLAGS);
	
#$(CXX) $(CXXFLAGS) batch_test.cc random.cc -o test $(LDFLAGS);
	
#$(CXX) $(CXXFLAGS) persistent_skip_list_test.cc random.cc -o test $(LDFLAGS);
