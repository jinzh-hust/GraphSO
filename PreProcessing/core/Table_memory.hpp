#ifndef MEMORY_H
#define MEMORY_H

#include <stdio.h>
#include <stdlib.h>
#include <math.h>
#include <unistd.h>
#include <malloc.h>
#include <omp.h>
#include <string.h>

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

class Table_memory{       //存chunk块的
	int size;
	int Vertex_num;
	int chunk_size;
	//指向首部的指针
	Entry *  mem;
	int parallelism;
	void ** pointer;
public:	
	Table_memory()
	{
		size = 0;
		chunk_size = 0;
		parallelism = 1;
		mem = NULL;
		pointer = NULL ;
	}
	int init(int chunk_size, int VertexId_num, int parallelism)
	{
		this->size = VertexId_num*sizeof(Entry);
		this->chunk_size = chunk_size;
		this->Vertex_num = VertexId_num;
		mem = (Entry *)calloc(VertexId_num,sizeof(Entry));
		assert(mem != NULL);
		int i = 0;
		int count = 0; //读完某个文件mem在哪,所有读完后应等于VertexId_num-1;
		string path = "chunk-table-";
		string * array = new string[parallelism];
		for(i=0;i<parallelism;i++)
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
		}
		printf("count:%d\n",count);
		return 0;
	}

	int FileSize(const char* fname)
	{
		struct stat statbuf;
		if(stat(fname,&statbuf)==0)
			return statbuf.st_size;
		return -1;
	}

	std::tuple<int, int, int, long> get_VertexId_message(VertexId ver)
	{
		for(int i=0;i<this->Vertex_num;i++)
		{
			if(ver == mem[i].source)
				return std::make_tuple(mem[i].number, mem[i].offset, mem[i].belong_part_j, mem[i].belong_part_i);
		}	
		return std::make_tuple(-1,-1,-1,-1);
	}
	
	~Table_memory()
	{
		size = 0;
		chunk_size = 0;
		free(mem);
	}
	
	
};






#endif
