#include <iostream>
#include <fstream>
#include <vector>
#include<sys/stat.h>
#include "./core/graph.hpp"
using namespace std;
int filesize(const char* fname)
{
     struct stat statbuf;
     if(stat(fname,&statbuf)==0)
        return statbuf.st_size;
}  

int main(int argc, char** argv)
{
 string path(argv[1]);
 cout << "read from " << path << endl;
 Entry * num;
 num = new Entry [300]; 
 ifstream file(path, ios::in | ios::binary);
if(!file)
{
 cout << "error" << endl;
 return 0;
}
 int i = 0;
 while(file.read((char *)&num[0],filesize(path.c_str())/sizeof(Entry)*sizeof(Entry)))   
 {

	i++;
 }
  	for(int p=0;p<=filesize(path.c_str())/sizeof(Entry)-1;p+=1)
	{
		cout <<num[p].range<< " "<<num[p].offset<<" "<<num[p].chunk_length<<" "<<num[p].id<<endl;
	}
 //file.close();
 return 0;
}
