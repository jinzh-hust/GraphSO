#ifndef MEMORY_H
#define MEMORY_H

#include <stdio.h>
#include <stdlib.h>
#include <math.h>
#include <unistd.h>
#include <malloc.h>
#include <omp.h>
#include <string.h>
#include <map>
//#include<io.h>

#include <iostream>
#include <fstream>

#include <thread>
#include <vector>
#include <functional>
#include <sys/stat.h>

#include "constants.hpp"
#include "type.hpp"
#include "bitmap.hpp"
#include "atomic.hpp"
#include "queue.hpp"
#include "partition.hpp"
#include "bigvector.hpp"
#include "time.hpp"
using namespace std;

class Chunk_memory{       //存chunk块的
	int size;
	int chunk_num;
	int chunk_size;
	int total_size;
	int *fsize;
	int parallelism;
	//指向首部的指针
	std::map<long,int> mem_table;//起始偏移量offset和其对应的内存的第int条边
	std::map<long,int>::iterator ob_mem_table;
	char *  mem_top;
	Entry ** chunk_table;  //设置1个元组存需要的chunk信息：1，chunk的包含节点数量，作为优先级的唯一依据。2，long存储偏移量。3为块长，4为块号
	std::map<float,std::pair<long, int>> set_table;    //把chunk中的大块组合起来。float是大块权重，pair存
	std::map <float,std::pair<long, int>>::iterator iter;
	int * file_table;       
	//void ** pointer;
public:	
	Chunk_memory()
	{
		size = 0;
		chunk_size = 0;
		total_size = 0;
		
		//parallelism = 1;
		mem_top = NULL;
		chunk_table = NULL;
		parallelism = std::thread::hardware_concurrency();
		fsize = new int[parallelism];
		//pointer = NULL ;
	}
	int init(int chunk_size, int parallelism, long mem_size,string path)
	{
		this->parallelism=parallelism;
		this->chunk_size = chunk_size;
		this->size = mem_size/chunk_size*chunk_size;
		//this->Vertex_num = VertexId_num;
		//mem = (Edge *)calloc(chunk_size/sizeof(Edge),sizeof(Edge));
		//assert(mem != NULL);
		int i = 0;
		int count = 0; 
		int read_bytes = 0;
		//string path = "chunk-table-";
		string * array = new string[parallelism];
		read_chunk_table(path);
		chunk_table_to_set_table(chunk_size);
		long PAGESIZE;
		PAGESIZE = 4096;
		try{
		mem_top= (char *)memalign(sizeof(Edge), size);;
        }
		catch(std::bad_alloc)
		{
			printf("unenough memorys.\n");
		}
		int read_mode;
		read_mode = O_RDONLY;
		assert(mem_top != NULL);
		int fin;
		fin = open((path+"/column").c_str(), read_mode);//开文件
		posix_fadvise(fin, 0, 0, POSIX_FADV_SEQUENTIAL);
		while(read_bytes<size)
		{
			for(iter = set_table.begin(); iter != set_table.end(); iter ++)
		    {
				if(read_bytes+(iter->second).second<size)
				{
					long bytes = pread(fin, mem_top+read_bytes, (iter->second).second, (iter->second).first);
					assert(bytes>0);
			        read_bytes+=bytes;
					mem_table.insert(std::pair<long,int>((iter->second).first , read_bytes));
				}

		    }
			break;
		}
		for(ob_mem_table=mem_table.begin();ob_mem_table!=mem_table.end();ob_mem_table++)
	    {
			printf("mem_table：%ld %d\n",ob_mem_table->first,ob_mem_table->second);
		}
		
		/* for(i=0;i<parallelism;i++)
		{
			array[i]= "chunk-table-";
		}
		for(i=0;i<parallelism;i++)
		{
			array[i]+=to_string(i);
		}
		int set_flag = 0;     
		for(i=0;i<parallelism;i++)
		{
	
			ifstream file(array[i], ios::in | ios::binary);
			if(!file)
			{
				cout << "error" << endl;
				return 0;
			}
			const char* fname = array[i].c_str();
			int fsize=FileSize(fname);
			 while(file.read((char *)&mem[set_flag],fsize))   
			{
				count++;
				//set_flag++;
			}
			set_flag+=fsize/sizeof(Entry);
			file.close();
		} */
		
		//printf("count:%d\n",count);
		return 0;
	}

	int FileSize(const char* fname)
	{
		struct stat statbuf;
		if(stat(fname,&statbuf)==0)
			return statbuf.st_size;
		return -1;
	}

    int read_chunk_table(string path)
	{
		total_size = 0;
		int i=0;
		string * array = new string[parallelism];
		for(i=0;i<parallelism;i++)
		{
			array[i]= path;
		}
		for(i=0;i<parallelism;i++)
		{
			array[i]+="/chunk-table-";
			array[i]+=to_string(i);
		}
		
		for(i=0;i<parallelism;i++)
		{
	
			ifstream file(array[i], ios::in | ios::binary);
			if(!file)
			{
				cout << "error" << endl;
				return 0;
			}
			const char* fname = array[i].c_str();
			//int fsize = 0;
			fsize[i] = FileSize(fname);
			printf("file size: %d\n",fsize[i]);
			total_size+=fsize[i];
			/*  while(file.read((char *)&mem[set_flag],fsize))   
			{
				count++;
				//set_flag++;
			}
			set_flag+=fsize/sizeof(Entry);
			file.close(); */
			file.close();
		}
		printf("total size: %d\n",total_size);
	    this->chunk_table = new Entry* [parallelism];//total_size/sizeof(Entry)
		assert(chunk_table!=NULL);
		for(i=0;i<parallelism;i++)
		{
			chunk_table[i]=new Entry[fsize[i]];
		}	
		int set_flag = 0;
		for(i=0;i<parallelism;i++)
		{
	
			ifstream file(array[i], ios::in | ios::binary);
			if(!file)
			{
				cout << "error" << endl;
				return 0;
			}
			const char* fname = array[i].c_str();
			//int fsize=FileSize(fname);
			Entry * buffer=chunk_table[i];
			printf("nmsl\n");
			 while(file.read((char *)buffer,fsize[i]))   
			{
				//count++;
				//set_flag++;
			}
			set_flag+=fsize[i];
			file.close();
		}
	//	printf("count:%d\n",count);
		return 0;
		
	}
	int chunk_table_to_set_table(int chunk_size)
	{
		int note_num = total_size/sizeof(Entry);
		int * range = new int[note_num];
		int * offset = new int[note_num];
		int * length = new int[note_num];
		int * id = new int[note_num];
		int i = 0;
		int n = 0;
		//printf("nmsl\n");
		Entry * buffer;
		buffer=chunk_table[0];
		printf("%ld\n",(buffer+3*sizeof(Entry))->offset);
		buffer=chunk_table[n];
		for(n=0;n<parallelism;n++)
		{
			Entry * buffer=chunk_table[n];
			for(;i<fsize[n]/sizeof(Entry) ;i++)
			{
				id[i] = buffer[i].id;
				range[i] = buffer[i].range;
				offset[i] = buffer[i].offset;
				length[i] = buffer[i].chunk_length;
				printf("%d %d %d %d\n",range[i] ,offset[i] ,length[i] ,id[i]);
			}
		}
		
		for(i = 0;i<note_num ;i++)
		{
			float rank = range[i];//!=0?1/range[i]:0.0;
			long new_offset = offset[i];
			int new_length = length[i];
			/* if( i!=0 && (offset{i-1}+length[i-1]) ==  offset[i] &&  (id[i-1]+1) == id[i] )     //合并条件
				{
					rank = range
				} */
			if (new_length==chunk_size)
			{
				set_table.insert(std::pair<float,std::pair<long, int>>(rank,std::pair<long,int>(new_offset,new_length)));
			}
		}
		for(iter = set_table.begin(); iter != set_table.end(); iter ++)
		    {
				printf("set_table: %f %ld %d\n",iter->first,iter->second.first,iter->second.second);
		    }
		return 0;
		
	}

    char* search(long offset)
	{
		if(mem_table.count(offset) > 0)
		{
		return this->mem_top+mem_table[offset];
		}
		return NULL;
	}
    

/* 	std::tuple<int, int, int, long> get_VertexId_message(VertexId ver)
	{
		for(int i=0;i<this->Vertex_num;i++)
		{
			if(ver == mem[i].source)
				return std::make_tuple(mem[i].number, mem[i].offset, mem[i].belong_part_j, mem[i].belong_part_i);
		}	
		return std::make_tuple(-1,-1,-1,-1);
	} */
	
	~Chunk_memory()
	{
		size = 0;
		chunk_size = 0;
		free(mem_top);
	}
	
	
};






#endif
