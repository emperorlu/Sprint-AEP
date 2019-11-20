#include <iostream>
#include <vector>
#include <list>
#include <string>
#include <cstdlib>
#include <cmath>
#include <algorithm>
#include "nvm_common2.h"
using namespace rocksdb;

class HashTable 
{
  public:
    // explicit HashTable(int size = 101);
    HashTable(int size = 30000);
    void display()const;

    void makeEmpty()
    {
        for(int i = 0; i < theLists.size(); i++)
            theLists[i].clear();
    }

    void traver()
    {
        for(int i = 0; i < theLists.size(); i++)
        {
            typename list<uint64_t>::iterator itr = theLists[i].begin();
            while(itr != theLists[i].end())
                cout << *itr++ << endl;
        }
    }

    bool contains(const uint64_t & x) 
    {
        list<uint64_t> & whichList = theLists[myhash(x)];
        return find(whichList.begin(), whichList.end(), x) != whichList.end();
    }

    bool remove(const uint64_t & x)
    {
        list<uint64_t> & whichList = theLists[myhash(x)];
        typename list<uint64_t>::iterator itr = find(whichList.begin(), whichList.end(), x);
        if(itr == whichList.end())
            return false;
        whichList.erase(itr);
        --currentSize;
        return true;
    }

    bool insert(const uint64_t & x)
    {
        // cout << "[DEBUG] insert  key: " << char8toint64(x.getName().c_str());
        // cout << "value: " << x.getValue() <<  endl;
        list<uint64_t> & whichList = theLists[myhash(x)];
        if(find(whichList.begin(), whichList.end(), x) != whichList.end())
            return false;
        whichList.push_back(x);
        ++currentSize;
        // if(++currentSize > theLists.size())
            // rehash();
            // makeEmpty();
        return true;
    }

    const string find_key(const uint64_t & x){
        // cout << "[DEBUG] find  key: " << char8toint64(x.getName().c_str()) <<  endl;
        list<uint64_t> & whichList = theLists[myhash(x)];
        typename list<uint64_t>::iterator itr = whichList.begin();
        while(itr != whichList.end()){
            if (*itr == x)
                return (*itr).getValue();
            itr++;
        }
        return "";
    }

    const uint64_t & f_key(const uint64_t & x){
        list<uint64_t> & whichList = theLists[myhash(x)];
        return *whichList.begin();
    }
    int getSize()
    {
        return currentSize;
    }
    int  currentSize;
  private:
    vector<list<uint64_t> > theLists;   // The array of Lists
    

    void rehash()
    {
        vector<list<uint64_t> > oldLists = theLists;

        // Create new double-sized, empty table
        theLists.resize(2 * theLists.size());
        for(int j = 0; j < theLists.size(); j++)
            theLists[j].clear();

        // Copy table over
        currentSize = 0;
        for(int i = 0; i < oldLists.size(); i++)
        {
            typename list<uint64_t>::iterator itr = oldLists[i].begin();
            while(itr != oldLists[i].end())
                insert(*itr++);
        }
    }

    int myhash(const uint64_t & x) const
    {
        int hashVal = 0;
        hashVal = x % theLists.size();
        if(hashVal < 0)
            hashVal += theLists.size();

        return hashVal;
    }
};


HashTable<uint64_t>::HashTable(int size)
{
    theLists = vector<list<uint64_t> >(size);
    currentSize = 0;
}


void HashTable<uint64_t>::display()const
{
    for(int i=0;i<theLists.size();i++)
    {
        cout<<i<<": ";
        typename std::list<uint64_t>::const_iterator iter = theLists[i].begin();
        while(iter != theLists[i].end())
        {
            cout<<*iter<<" ";
            ++iter;
        }
        cout<<endl;
    }
}



