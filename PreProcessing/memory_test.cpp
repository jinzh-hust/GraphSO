#include <iostream>
#include <fstream>
#include <vector>
#include "./core/graph.hpp"
#include "./core/memory_control.hpp"
using namespace std;
  
int main(int argc, char** argv)
{
 string path(argv[1]);
 long memory_bytes = atol(argv[2])*1024l*1024l;    //内存大小,单位MB
 long offset=atol(argv[3]);
 
 int parallelism = std::thread::hardware_concurrency();
 //cout << "read from " << path << endl;
 
 Chunk_memory chunk_memory;
// chunk_memory.read_chunk_table(path);
 //chunk_memory.chunk_table_to_set_table(4980736);
 chunk_memory.init(4980736,parallelism,memory_bytes,path);
if(chunk_memory.search(offset)==0)
{
     return 0;
}
 Edge & e = *(Edge*)chunk_memory.search(offset);
 printf("%d %d\n",e.source,e.target);
 Edge & e1 = *(Edge*)(chunk_memory.search(offset)+sizeof(VertexId)*2);
  printf("%d %d\n",e1.source,e1.target);
 printf("%d\n",sizeof(Edge));
  printf("%d\n",sizeof(VertexId)*2);
/*  Entry  num;
 num = new Entry [300]; 
 ifstream file(path, ios::in | ios::binary);
if(!file)
{
 cout << "error" << endl;
 return 0;
}
 int i = 0;
 while(file.read((char *)&num[0],300*sizeof(Entry)))   
 {

	i++;
 }
  	for(int p=0;p<=299;p+=1)
	{
		cout <<num[p].source<< " " << num[p].number<<" "<<num[p].offset<< " "<<num[p].belong_part_j<<" "<<num[p].belong_part_i<<endl;
	}
 //file.close(); */
 return 0;
}
