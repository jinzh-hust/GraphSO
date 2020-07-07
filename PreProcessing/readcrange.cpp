#include <iostream>
#include <fstream>
#include <vector>
using namespace std;
  
int main(int argc, char** argv)
{
 string path(argv[1]);
 cout << "read from " << path << endl;
 int * num;
 num = new int [400]; 
 ifstream file(path, ios::in | ios::binary);
if(!file)
{
 cout << "error" << endl;
 return 0;
}
 int i = 0;
 while(file.read((char *)&num[0],400*sizeof(int)))   
 {

	i++;
 }
  	for(int p=0;p<=100;p+=4)
	{
		cout <<num[p]<< " " << num[p+1]<<" "<<num[p+2]<<" "<<num[p+3]<<" "<<endl;
	}
 //file.close();
 return 0;
}
