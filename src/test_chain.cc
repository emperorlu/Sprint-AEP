#include <iostream>
#include "rang_chain.h"


int main(){
    int ops = 120;
    int max = 90;
    RangChain<int> Crchain(10);
    RangChain<int> Hrchain(10);
    Crchain.initialize(50, 120);
    Hrchain.initialize(0, 80);
    for(int i = 50; i < ops; i++){
        Crchain.insert(i, i);
    }
    for(int i = 0; i < 189; i++){
        Hrchain.insert(i, i);    
    }

    cout<< "Crchain: " <<endl;
    Crchain.traver();

    cout<< "Hrchain: " <<endl;
    Hrchain.traver();
    if(Hrchain.maxhot >= Crchain.minhot)
        Crchain.change(max, Hrchain);
    
    Crchain.traver();
}
