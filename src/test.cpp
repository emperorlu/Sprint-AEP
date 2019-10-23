#include <iostream>
#include <string>
#include <algorithm>
#include <vector>

using namespace std;

void findNum(vector<int> data, int* num1, int* num2);
int main(){
    int input[10]={2,4,3,6,3,2,5,5,6,8};
    vector<int> data(input, input+10);
    int *num1;
    num1 = (int*)new int;
    int *num2;
    num2 = (int*)new int;
    findNum(data, num1, num2);
    cout<<"final result num1 is: "<<*num1<<endl;
    cout<<"final result num2 is: "<<*num2<<endl;
    return 0;
}
void findNum(vector<int> data, int* num1, int* num2)
{
    int count=0;
    bool flag;
    int *p1=num1;
    int *p2=num2;
    sort(data.begin(), data.end());
    while(count<2)
    {
        flag=false;
        for(int i=1; i<4&&i<data.size(); i++)
        {
            if(data[0] == data[i])
            {
                cout<<"erase data is: "<<data[0]<<endl;
                data.erase(data.begin());
                data.erase(data.begin()+i-1);
                flag=true;
                break;
            }
        }
        if(!flag)
        {    
            if(count == 0)
            {
                cout<<"data[0] has been printed!"<<data[0]<<endl;
                //这里下一句对指针赋值会崩
                *p1=data[0];
                cout<<"num1 is : "<<*p1<<endl;
                data.erase(data.begin());
                count++;
                continue;
            }
            if(count == 1)
            {
                //这里下一句对指针赋值会崩
                *p2=data[0];
                cout<<"num2 is : "<<*p2<<endl;
                count++;
            }
        }
    }
    return;

}


