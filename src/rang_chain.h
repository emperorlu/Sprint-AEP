#include <iostream>
#include <vector>
#include <list>
#include <string>
#include <cstdlib>
#include <cmath>
#include <algorithm>
using namespace std;

template <class T>
class RangChain 
{
  public:
    RangChain()
    {
        listSize = 10;
        theLists = vector<list<T> >(listSize);
        myList = vector<list<T> >(listSize);
        currentSize = 0;
        maxhot = 10;
        minhot = 0; 
    }

    void initialize(int minValue, int maxValue)
    {
        maxhot = maxValue;
        minhot = minValue;
        // hc = sign;
        cout << "listSize:" << listSize << endl;
        cout << "maxhot:" << maxhot << "minhot:" << minhot << endl;
    }

    int getSize()
    {
        return currentSize;
    }

    void makeEmpty()
    {
        for(int i = 0; i < theLists.size(); i++)
            theLists[i].clear();
    }

    void traver()
    {
        for(int i = 0; i < theLists.size(); i++)
        {
            cout << i << ":";
            typename list<T>::iterator itr = theLists[i].begin();
            while(itr != theLists[i].end())
                cout << *itr++ << "\t";
            cout << endl;
        }
    }
    void change(int max, RangChain Hrchain)
    {   
        vector<list<T> > clist = Hrchain.theLists; 
        for(int i = clist.size()-1; i >= 0 ; i--)
        {
            while(getSize() >= max){
                remove();
            }
            cout<< i << "begin!" << endl;
            traver();
            // cout << i << ":";
            typename list<T>::iterator itr = clist[i].begin();
            while(itr != clist[i].end())
                {   
                    cout<< "value: " << *itr << endl;
                    if(*itr < minhot){
                        cout<< "over!" << endl;
                        traver();
                        return;
                    }
                    insert(*itr, *itr);
                    itr++;
                }
            // cout << endl;
            traver();
            cout << "size: " << getSize() << endl;
        } 
        
    }
    // bool contains(const T & x) 
    // {
    //     list<T> & whichList = theLists[myid(x)];
    //     return find(whichList.begin(), whichList.end(), x) != whichList.end();
    // }
    bool reset(const T & x, int valueold, int value)
    {
        list<T> & whichList = theLists[myid(valueold)];
        typename list<T>::iterator itr = find(whichList.begin(), whichList.end(), x);
        if(itr == whichList.end())
            return false;
        whichList.erase(itr);
        insert(x, value);
        return true;
    }

    bool remove()
    {   
        int i = 0;
        while (theLists[i].size() == 0){
            i++;
        }
        list<T> & whichList = theLists[i];
        cout << "[Debug] delete:" << *whichList.begin() << endl;
        whichList.erase(whichList.begin());
        currentSize--;
        return true;
    }
    
    void relist()
    {   
        // cout << "before relist: " << endl;
        traver();
        for(int i = 0; i < theLists.size(); i++)
        {
            // cout << i << ":";
            typename list<T>::iterator itr = theLists[i].begin();
            while(itr != theLists[i].end()){
                myinsert(*itr, *itr);
                itr++;
            }
        }
        theLists = myList;
        // cout << "relist: " << endl;
        traver();
    }

    bool insert(const T & x, int value)
    {    
        if (value < minhot){
            minhot = value;
            relist();
        }else if (value >= maxhot)
        {
            maxhot = value + 10;
            relist();
        }
        cout << "[Debug] insert:" << myid(value) << "value:" << value << endl;
        list<T> & whichList = theLists[myid(value)];
        whichList.push_front(x);
        currentSize++;
        return true;   
    }

    int  maxhot, minhot;
    vector<list<T> > theLists;   // The array of Lists
    
  private:
    int listSize;
    int currentSize;
    vector<list<T>>  myList;
    // int hc; //c or h

    int myid(int value)
    {
        int kua = (maxhot - minhot) / listSize;
        int rank = (value - minhot) / kua;
        return rank;
    }

    bool myinsert(const T & x, int value)
    {   
        cout << "[Debug] myinsert:" << myid(value) << "value:" << value << endl;
        list<T> & whichList = myList[myid(value)];
        whichList.push_front(x);
        currentSize++;
        return true;   
    }

};


// template <class T>
// void RangChain<T>::display()const
// {
//     for(int i=0;i<theLists.size();i++)
//     {
//         cout<<i<<": ";
//         typename std::list<T>::const_iterator iter = theLists[i].begin();
//         while(iter != theLists[i].end())
//         {
//             cout<<*iter<<" ";
//             ++iter;
//         }
//         cout<<endl;
//     }
// }





