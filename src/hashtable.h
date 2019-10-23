#include <iostream>
#include <vector>
#include <list>
#include <string>
#include <cstdlib>
#include <cmath>
#include <algorithm>
#include "nvm_common2.h"
using namespace rocksdb;

template <class HashedObj>
class HashTable 
{
  public:
    // explicit HashTable(int size = 101);
    HashTable(int size = 101);
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
            typename list<HashedObj>::iterator itr = theLists[i].begin();
            while(itr != theLists[i].end())
                cout << *itr++ << endl;
        }
    }

    bool contains(const HashedObj & x) 
    {
        list<HashedObj> & whichList = theLists[myhash(x)];
        return find(whichList.begin(), whichList.end(), x) != whichList.end();
    }

    bool remove(const HashedObj & x)
    {
        list<HashedObj> & whichList = theLists[myhash(x)];
        typename list<HashedObj>::iterator itr = find(whichList.begin(), whichList.end(), x);
        if(itr == whichList.end())
            return false;
        whichList.erase(itr);
        --currentSize;
        return true;
    }

    bool insert(const HashedObj & x)
    {
        // cout << "[DEBUG] insert  key: " << char8toint64(x.getName().c_str());
        // cout << "value: " << x.getValue() <<  endl;
        list<HashedObj> & whichList = theLists[myhash(x)];
        if(find(whichList.begin(), whichList.end(), x) != whichList.end())
            return false;
        whichList.push_back(x);
        ++currentSize;
        // if(++currentSize > theLists.size())
            // rehash();
            // makeEmpty();
        return true;
    }

    const string find_key(const HashedObj & x){
        // cout << "[DEBUG] find  key: " << char8toint64(x.getName().c_str()) <<  endl;
        list<HashedObj> & whichList = theLists[myhash(x)];
        typename list<HashedObj>::iterator itr = whichList.begin();
        while(itr != whichList.end()){
            if (*itr == x)
                return (*itr).getValue();
            itr++;
        }
        return "";
    }

    const HashedObj & f_key(const HashedObj & x){
        list<HashedObj> & whichList = theLists[myhash(x)];
        return *whichList.begin();
    }
    int getSize()
    {
        return currentSize;
    }

  private:
    vector<list<HashedObj> > theLists;   // The array of Lists
    int  currentSize;

    void rehash()
    {
        vector<list<HashedObj> > oldLists = theLists;

        // Create new double-sized, empty table
        theLists.resize(2 * theLists.size());
        for(int j = 0; j < theLists.size(); j++)
            theLists[j].clear();

        // Copy table over
        currentSize = 0;
        for(int i = 0; i < oldLists.size(); i++)
        {
            typename list<HashedObj>::iterator itr = oldLists[i].begin();
            while(itr != oldLists[i].end())
                insert(*itr++);
        }
    }

    int myhash(const HashedObj & x) const
    {
        int hashVal = 0;
        string key = x.getName();
        for(int i = 0; i < key.length(); i++)
            hashVal = 37 * hashVal + key[i];

        hashVal %= theLists.size();
        if(hashVal < 0)
            hashVal += theLists.size();

        return hashVal;
    }
};

template <class T>
HashTable<T>::HashTable(int size)
{
    theLists = vector<list<T> >(size);
    currentSize = 0;
}

template <class T>
void HashTable<T>::display()const
{
    for(int i=0;i<theLists.size();i++)
    {
        cout<<i<<": ";
        typename std::list<T>::const_iterator iter = theLists[i].begin();
        while(iter != theLists[i].end())
        {
            cout<<*iter<<" ";
            ++iter;
        }
        cout<<endl;
    }
}

class Employee
{
public:
    Employee(){}
    Employee(const string n,int s=0):name(n),salary(s){ }
    const string & getName()const  { return name; }
    const int & getValue()const  { return salary; }
    bool operator == (const Employee &rhs) const
    {
        return getName() == rhs.getName();
    }
    bool operator != (const Employee &rhs) const
    {
        return !(*this == rhs);
    }
    // friend ostream& operator <<(ostream& out,const Employee& e)
    // {
    //     out<<"("<<e.name<<","<<e.salary<<") ";
    //     return out;
    // }
private:
    string name;
    int salary;
};

class Keyvalue
{
public:
    Keyvalue(){}
    Keyvalue(const string n, const string s):key(n),value(s){ }
    const string & getName()const  { return key; }
    const string & getValue()const  { return value; }
    bool operator == (const Keyvalue &rhs) const
    {
        // bool r = getName() == rhs.getName();
        // cout << "[Debug] operator ==:  " << r << endl;
        bool r = strncmp(getName().c_str(), rhs.getName().c_str(), NVM_KeySize);
        return !r;
        // return getName() == rhs.getName();
    }
    bool operator != (const Keyvalue &rhs) const
    {
        return !(*this == rhs);
    }
    friend ostream& operator <<(ostream& out,const Keyvalue& e)
    {
        out<<"("<<e.key<<","<<e.value<<") ";
        return out;
    }
private:
    string key;
    string value;
};

