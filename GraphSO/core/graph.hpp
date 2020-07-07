
#ifndef GRAPH_H
#define GRAPH_H

#include <stdio.h>
#include <stdlib.h>
#include <math.h>
#include <unistd.h>
#include <malloc.h>
#include <omp.h>
#include <string.h>

#include <thread>
#include <vector>
#include <functional>

#include "constants.hpp"
#include "type.hpp"
#include "bitmap.hpp"
#include "atomic.hpp"
#include "queue.hpp"
#include "partition.hpp"
#include "bigvector.hpp"
#include "time.hpp"

#include "memory_control.hpp"
bool f_true(VertexId v) {
	return true;
}

void f_none_1(std::pair<VertexId,VertexId> vid_range) {

}

void f_none_2(std::pair<VertexId,VertexId> source_vid_range, std::pair<VertexId,VertexId> target_vid_range) {

}

class Graph {
	int edge_unit;
	long ** fsize;
	char ** buffer_pool;
	long * column_offset;
	long * row_offset;
	long memory_bytes;
	int * range_table;    //各个分区所有的节点范围
	int partition_batch;
	long vertex_data_bytes;
	long PAGESIZE;

	long block_size;
public:
	std::string path;
	int parallelism;

	bool * should_access_shard;
	bool * should_access_shard_pagerank;
	bool * should_access_shard_sssp;
	bool * should_access_shard_bfs;
	bool * should_access_shard_wcc;

	int edge_type;
	VertexId vertices;
	VertexId * startv;
	VertexId * endv;
	int * sub_tablea;
	int * sub_tableb;
	EdgeId edges;
	int partitions;

	Graph (std::string path) {
		PAGESIZE = 4096;
		parallelism = std::thread::hardware_concurrency();
		//parallelism = 1;
		buffer_pool = new char * [parallelism*1];
		for (int i=0;i<parallelism*1;i++) {
			buffer_pool[i] = (char *)memalign(PAGESIZE, IOSIZE);
			assert(buffer_pool[i]!=NULL);
			memset(buffer_pool[i], 0, IOSIZE);
		}
		init(path);
	}

	void set_memory_bytes(long memory_bytes) {
		this->memory_bytes = memory_bytes;
	}

	void set_vertex_data_bytes(long vertex_data_bytes) {
		this->vertex_data_bytes = vertex_data_bytes;
	}

	void init(std::string path) {
		this->path = path;

		FILE * fin_meta = fopen((path+"/meta").c_str(), "r");
		fscanf(fin_meta, "%d %d %ld %d", &edge_type, &vertices, &edges, &partitions);
		fclose(fin_meta);

		if (edge_type==0) {
			PAGESIZE = 4096;
		} else {
			PAGESIZE = 12288;
		}

		should_access_shard = new bool[partitions];
		should_access_shard_pagerank = new bool[partitions];
		should_access_shard_wcc = new bool[partitions];
		should_access_shard_sssp = new bool[partitions];
		should_access_shard_bfs = new bool[partitions];

		if (edge_type==0) {
			edge_unit = sizeof(VertexId) * 2;
		} else {
			edge_unit = sizeof(VertexId) * 2 + sizeof(Weight);
		}

		memory_bytes = 1024l*1024l*1024l*1024l; // assume RAM capacity is very large
		partition_batch = partitions;
		vertex_data_bytes = 0;

		char filename[1024];
		fsize = new long * [partitions];
		for (int i=0;i<partitions;i++) {
			fsize[i] = new long [partitions];
			for (int j=0;j<partitions;j++) {
				sprintf(filename, "%s/block-%d-%d", path.c_str(), i, j);
				fsize[i][j] = file_size(filename);
			}
		}

		long bytes;
        range_table = new int [partitions*partitions+1];
		
		column_offset = new long [partitions*partitions+1];
		int fin_column_offset = open((path+"/column_offset").c_str(), O_RDONLY);
		bytes = read(fin_column_offset, column_offset, sizeof(long)*(partitions*partitions+1));
		assert(bytes==sizeof(long)*(partitions*partitions+1));
		close(fin_column_offset);

		row_offset = new long [partitions*partitions+1];
		int fin_row_offset = open((path+"/row_offset").c_str(), O_RDONLY);
		bytes = read(fin_row_offset, row_offset, sizeof(long)*(partitions*partitions+1));
		assert(bytes==sizeof(long)*(partitions*partitions+1));
		close(fin_row_offset);

		startv = new VertexId [partitions*partitions];
		endv = new VertexId [partitions*partitions];
		sub_tablea = new int [partitions*partitions];
		sub_tableb = new int [partitions*partitions];
		int a=0;
		int b=0;
		for(a=0;a<partitions*partitions;a++)
			{
				startv[a] = 2147483646;
				endv[a] = 0;
			}
		set_range_table();

		for(a=0;a<partitions*partitions;a++)
		{							
			range_table[a]= endv[a] - startv[a] ;
			if(a==0)
			{
				sub_tablea[a]=0;
				sub_tablea[a]=0;
			}
			else
			{
				sub_tablea[a]=startv[a]-startv[a-1];
				sub_tableb[a]=endv[a]-endv[a-1];
			}
		}
	    for(a=0;a<partitions*partitions;a++)
		{
			printf("startv[%d]:%d\n",a,startv[a]);
			//b+=range_table[a];
		}
	    for(a=0;a<partitions*partitions;a++)
		{
			printf("endv[%d]:%d\n",a,endv[a]);
			//b+=range_table[a];
		}
	    for(a=0;a<partitions*partitions;a++)
		{
			printf("sub_tablea[%d]:%d\n",a,sub_tablea[a]);
			//b+=range_table[a];
		}
	    for(a=0;a<partitions*partitions;a++)
		{
			printf("sub_tableb[%d]:%d\n",a,sub_tableb[a]);
			//b+=range_table[a];
		}
		for(a=0;a<partitions*partitions;a++)
		{
			printf("rangetable[%d]:%d\n",a,range_table[a]);
			b+=range_table[a];
		}
		printf("v__num:%d\n",b);
	}

	Bitmap * alloc_bitmap() {
		return new Bitmap(vertices);      //节点数大小的bitmap
	}

	template <typename T>
	T stream_vertices(std::function<T(VertexId)> process, Bitmap * bitmap = nullptr, T zero = 0,
		std::function<void(std::pair<VertexId,VertexId>)> pre = f_none_1,
		std::function<void(std::pair<VertexId,VertexId>)> post = f_none_1) {
		T value = zero;
		if (bitmap==nullptr && vertex_data_bytes > (0.8 * memory_bytes)) {
			for (int cur_partition=0;cur_partition<partitions;cur_partition+=partition_batch) {
				VertexId begin_vid, end_vid;
				begin_vid = get_partition_range(vertices, partitions, cur_partition).first;
				if (cur_partition+partition_batch>=partitions) {
					end_vid = vertices;               //最后一个进程，节点为max节点
				} 
				else {
					end_vid = get_partition_range(vertices, partitions, cur_partition+partition_batch).first;           //下一个进程起点
				}
				pre(std::make_pair(begin_vid, end_vid));             //对块begin-end的pair预处理
				#pragma omp parallel for schedule(dynamic) num_threads(parallelism)
				for (int partition_id=cur_partition;partition_id<cur_partition+partition_batch;partition_id++) {
					if (partition_id < partitions) {
						T local_value = zero;
						VertexId begin_vid, end_vid;
						std::tie(begin_vid, end_vid) = get_partition_range(vertices, partitions, partition_id);
						for (VertexId i=begin_vid;i<end_vid;i++) {
							local_value += process(i);      //处理
						}
						write_add(&value, local_value);
					}
				}
				#pragma omp barrier
				post(std::make_pair(begin_vid, end_vid));         //对块begin-end的pair收尾处理   
			}
		} 
		else {
			#pragma omp parallel for schedule(dynamic) num_threads(parallelism)
			for (int partition_id=0;partition_id<partitions;partition_id++) {
				T local_value = zero;
				VertexId begin_vid, end_vid;
				std::tie(begin_vid, end_vid) = get_partition_range(vertices, partitions, partition_id);
				if (bitmap==nullptr) {
					for (VertexId i=begin_vid;i<end_vid;i++) {
						local_value += process(i);
					}
				} 
				else {
					VertexId i = begin_vid;
					while (i<end_vid) {
						unsigned long word = bitmap->data[WORD_OFFSET(i)];
						if (word==0) {
							i = (WORD_OFFSET(i) + 1) << 6;
							continue;
						}
						size_t j = BIT_OFFSET(i);
						word = word >> j;
						while (word!=0) {
							if (word & 1) {
								local_value += process(i);
							}
							i++;
							j++;
							word = word >> 1;
							if (i==end_vid) break;
						}
						i += (64 - j);
					}
				}
				write_add(&value, local_value);
			}
			#pragma omp barrier
		}
		return value;
	}

	void set_partition_batch(long bytes) {
		int x = (int)ceil(bytes / (0.8 * memory_bytes));
		partition_batch = partitions / x;
	}
	
    int set_range_table() {             //结论1:bitmap在预处理后的streamedges没有卵用
		int value = 0;
		Queue<std::tuple<int, long, long, int ,int> > tasks(65536);
		std::vector<std::thread> threads;
		long read_bytes = 0;

		long total_bytes = 0;
		for (int i=0;i<partitions;i++) {
			//if (!should_access_shard[i]) continue;//continue 会跳过当前循环中的代码，强迫开始下一次循环。
			for (int j=0;j<partitions;j++) {
				total_bytes += fsize[i][j];    //fsize（物理划分的大小）
			}
		}
		int read_mode;
		if (memory_bytes < total_bytes) {
			read_mode = O_RDONLY | O_DIRECT;
			// printf("use direct I/O\n");
		} else {
			read_mode = O_RDONLY;
			           // 确定printf("use buffered I/O\n");
		}

		int fin;
		long offset = 0;
		int update_mode = 0;
		long block_size_threads = (block_size/parallelism)/ edge_unit * edge_unit;
		//printf("block_size_threads: %ld\n", block_size_threads);
		switch(update_mode) {
		case 0: // source oriented update（源定向更新（工作准备））     两种遍历block的方式一种行优先一种列优先（这个大概是行优先）
			threads.clear();    //清空线程
			for (int ti=0;ti<parallelism;ti++) {
				threads.emplace_back([&](int thread_id){       //插入一个新的线程
					int local_value = 0;
					long local_read_bytes = 0;
					while (true) {
						int fin, partition_id,chunkmark;
						long offset, length;
						std::tie(fin, offset, length, partition_id, chunkmark) = tasks.pop();
						if (fin==-1) break;
						char * buffer = buffer_pool[thread_id];     //buffer_pool页表池子
						long bytes = pread(fin, buffer, length, offset);          //成功时返回读取的字节数      offset偏移量 读取地址=文件开始+offset。注意，执行后，文件偏移指针不变
						assert(bytes>0);
						local_read_bytes += bytes;
						// CHECK: start position should be offset % edge_unit
						for (long pos=offset % edge_unit;pos+edge_unit<=bytes;pos+=edge_unit) {
							Edge & e = *(Edge*)(buffer+pos);          //读出的边
							write_min(&startv[partition_id*partitions+chunkmark],e.source);
							write_max(&endv[partition_id*partitions+chunkmark],e.source);
							//if (bitmap==nullptr || bitmap->get_bit(e.source)) {
							//	local_value += process_pagerank(e);       //第一个函数的并发执行
							//}
						}
					}
					write_add(&value, local_value);    //确保原子性
					write_add(&read_bytes, local_read_bytes);
				}, ti);
			}
			fin = open((path+"/row").c_str(), read_mode);
			posix_fadvise(fin, 0, 0, POSIX_FADV_SEQUENTIAL);    //清理缓存
			for (int i=0;i<partitions;i++) {
				//if (!should_access_shard[i]) continue;
				for (int j=0;j<partitions;j++) {                      //Pji表示第j个作业共享的已加载图形结构分区Pi
					long begin_offset = row_offset[i*partitions+j];
					if (begin_offset - offset >= PAGESIZE) {
						offset = begin_offset / PAGESIZE * PAGESIZE;        
					}
					long end_offset = row_offset[i*partitions+j+1];
					if (end_offset <= offset) continue;
					while (end_offset - offset >= IOSIZE) {
						tasks.push(std::make_tuple(fin, offset, IOSIZE, i, j));
						offset += IOSIZE;
					}
					if (end_offset > offset) {
						tasks.push(std::make_tuple(fin, offset, (end_offset - offset + PAGESIZE - 1) / PAGESIZE * PAGESIZE, i, j));
						offset += (end_offset - offset + PAGESIZE - 1) / PAGESIZE * PAGESIZE;
					}
				}
			}
			for (int i=0;i<parallelism;i++) {
				tasks.push(std::make_tuple(-1, 0, 0, -1, -1));
			}
			for (int i=0;i<parallelism;i++) {
				threads[i].join();
			}
			break;
		case 1: // target oriented update     591579114
			fin = open((path+"/column").c_str(), read_mode);
			posix_fadvise(fin, 0, 0, POSIX_FADV_SEQUENTIAL);

			for (int cur_partition=0;cur_partition<partitions;cur_partition+=partition_batch) {
				VertexId begin_vid, end_vid;
				begin_vid = get_partition_range(vertices, partitions, cur_partition).first;
				if (cur_partition+partition_batch>=partitions) {
					end_vid = vertices;
				} else {
					end_vid = get_partition_range(vertices, partitions, cur_partition+partition_batch).first;
				}
			//	pre_source_window(std::make_pair(begin_vid, end_vid));
				// printf("pre %d %d\n", begin_vid, end_vid);
				threads.clear();

				for (int ti=0;ti<parallelism;ti++) {
					threads.emplace_back([&](int thread_id){
						int local_value = 0;
						long local_read_bytes = 0;
						while (true) {
							int fin, partition_id,chunkmark;
							long offset, length;

							std::tie(fin, offset, length, partition_id, chunkmark) = tasks.pop();
							if (fin==-1) break;
							char * buffer = buffer_pool[thread_id];
							long bytes = pread(fin, buffer, length, offset);
							assert(bytes>0);
							local_read_bytes += bytes;
							// CHECK: start position should be offset % edge_unit
                            long pos;
							for (pos=offset % edge_unit; pos+block_size_threads<=bytes; pos+=block_size_threads) {
                                //if(should_access_shard_pagerank[partition_id])
                                    for(long pos_block = pos; pos_block+edge_unit<=pos+block_size_threads; pos_block+=edge_unit){
                                        Edge & e = *(Edge*)(buffer+pos_block);
										write_min(&startv[chunkmark*partition_batch+partition_id],e.source);
										write_max(&endv[chunkmark*partition_batch+partition_id],e.source);
                                     /* if(startv[chunkmark*partitions+partition_id] >= e.source)
											{
												startv[chunkmark*partition_batch+partition_id] = e.source;  //表的开始源节点j*i(一个循环的大小)+i
											}
										if(endv[chunkmark*partitions+partition_id] <= e.source)
											{
												endv[chunkmark*partition_batch+partition_id] = e.source;  //表的开始源节点
											} */
										
	
                                        //process_pagerank(e);
                                    }
									//range_table[chunkmark*partitions+partition_id] = endv - startv;
									
							}//最后一个之前的循环处理
                            //printf("range%d:%d-%d,size:%d\n",cur_partition*partition_batch+partition_id,startv,endv,endv - startv);
						   // range_table[thread_id+partition_id*partitions] = endv - startv;
							if(pos < bytes){
                                //if(should_access_shard_pagerank[partition_id])
                                    for(long pos_block = pos; pos_block+edge_unit<=bytes; pos_block+=edge_unit){
                                        Edge & e = *(Edge*)(buffer+pos_block);
										write_min(&startv[chunkmark*partition_batch+partition_id],e.source);
										write_max(&endv[chunkmark*partition_batch+partition_id],e.source);
										/* if(startv[chunkmark*partitions+partition_id] >= e.source)
											{
												startv[chunkmark*partition_batch+partition_id] = e.source;  //表的开始源节点
											}
										if(endv[chunkmark*partitions+partition_id] <= e.source)
											{
												endv[chunkmark*partition_batch+partition_id] = e.source;  //表的开始源节点
											} */
										
											//range_table[cur_partition+partition_id*partitions] = endv - startv;
                                        //process_pagerank(e);
                                    }
                            //printf("range%d:%d-%d,size:%d\n",cur_partition*partition_batch+partition_id,startv,endv,endv - startv);
						    //range_table[chunkmark*partitions+partition_id] = endv - startv;
							}
					    //startv = 2147483646;
					    //endv = 0;
						}


						write_add(&value, local_value);
						write_add(&read_bytes, local_read_bytes);
					}, ti);
				}
				offset = 0;
				for (int j=0;j<partitions;j++) {
					for (int i=cur_partition;i<cur_partition+partition_batch;i++) {
						if (i>=partitions) break;
						//if (!should_access_shard[i]) continue;
						long begin_offset = column_offset[j*partitions+i];
						/*if (begin_offset - offset >= PAGESIZE) {
							offset = begin_offset / PAGESIZE * PAGESIZE;
						}*/
						offset = begin_offset;

						long end_offset = column_offset[j*partitions+i+1];
						if (end_offset <= offset) continue;
						while (end_offset - offset >= IOSIZE) {
							tasks.push(std::make_tuple(fin, offset, IOSIZE, i ,j));
							offset += IOSIZE;
						}
						if (end_offset > offset) {
							tasks.push(std::make_tuple(fin, offset, end_offset - offset, i, j));
							offset += (end_offset - offset);
						}
					}
				}
				for (int i=0;i<parallelism;i++) {
					tasks.push(std::make_tuple(-1, 0, 0, -1, -1));
				}
				for (int i=0;i<parallelism;i++) {
					threads[i].join();
				}

				//post_source_window(std::make_pair(begin_vid, end_vid));
				// printf("post %d %d\n", begin_vid, end_vid);
				
				
				
			}

			break;
		default:
			assert(false);
		}

		close(fin);
		// printf("streamed %ld bytes of edges\n", read_bytes);
		return value;
	}
	template <typename... Args>
	void hint(Args... args);

	template <typename A>
	void hint(BigVector<A> & a) {
		long bytes = sizeof(A) * a.length;
		set_partition_batch(bytes);
	}

	template <typename A, typename B>
	void hint(BigVector<A> & a, BigVector<B> & b) {
		long bytes = sizeof(A) * a.length + sizeof(B) * b.length;
		set_partition_batch(bytes);
	}

	template <typename A, typename B, typename C>
	void hint(BigVector<A> & a, BigVector<B> & b, BigVector<C> & c) {
		long bytes = sizeof(A) * a.length + sizeof(B) * b.length + sizeof(C) * c.length;
		set_partition_batch(bytes);
	}



	void set_sizeof_blocks(long cache_size, long graph_size, long specific_size){
	    //printf("cache_size: %ld, graph_size: %ld, specific_size: %ld, edge_unit: %ld\n", cache_size, graph_size, specific_size, edge_unit);
        long block_s = cache_size/(1.0+ (specific_size *1.0) / graph_size);
        //printf("block_s: %ld\n", block_s);
        this->block_size = (block_s/edge_unit)*edge_unit;
        printf("block_size: %ld\n", this->block_size);
    }


    void clear_should_access_shard(bool * this_should_access_shard){
        for (int i=0;i<partitions;i++) {
				this_should_access_shard[i] = false;
        }
    }

	void get_should_access_shard(bool * this_should_access_shard, Bitmap * bitmap = nullptr){
	    if (bitmap==nullptr) {
			for (int i=0;i<partitions;i++) {
				this_should_access_shard[i] = true;          //空指针时都可以有
			}
		} else {
			#pragma omp parallel for schedule(dynamic) num_threads(parallelism)
			for (int partition_id=0;partition_id<partitions;partition_id++) {
				VertexId begin_vid, end_vid;
				std::tie(begin_vid, end_vid) = get_partition_range(vertices, partitions, partition_id);
				VertexId i = begin_vid;
				while (i<end_vid) {
					unsigned long word = bitmap->data[WORD_OFFSET(i)];
					if (word!=0) {                   //检查word是否为0，有1
						this_should_access_shard[partition_id] = true;
						break;
					}
					i = (WORD_OFFSET(i) + 1) << 6;
				}
			}
			#pragma omp barrier
		}
	}

	void get_global_should_access_shard(bool * should_access_shard_pagerank, bool * should_access_shard_wcc,
                                     bool * should_access_shard_sssp, bool * should_access_shard_bfs){
        for (int i=0;i<partitions;i++) {
            if(should_access_shard_pagerank[i]){
                should_access_shard[i] = true;
                continue;
            }
            if(should_access_shard_wcc[i]){
                should_access_shard[i] = true;
                continue;
            }
            if(should_access_shard_sssp[i]){
                should_access_shard[i] = true;
                continue;
            }
            if(should_access_shard_bfs[i]){
                should_access_shard[i] = true;
                continue;
            }
        }
	}

	template <typename T>
	T stream_edges(std::function<T(Edge&)> process_pagerank, std::function<T(Edge&)> process_sssp,
                std::function<T(Edge&)> process_bfs,std::function<T(Edge&)> process_wcc,
                Bitmap * bitmap = nullptr, T zero = 0, int update_mode = 1, Chunk_memory * chunk_memory = nullptr,
		std::function<void(std::pair<VertexId,VertexId> vid_range)> pre_source_window = f_none_1,
		std::function<void(std::pair<VertexId,VertexId> vid_range)> post_source_window = f_none_1,
		std::function<void(std::pair<VertexId,VertexId> vid_range)> pre_target_window = f_none_1,
		std::function<void(std::pair<VertexId,VertexId> vid_range)> post_target_window = f_none_1) {               //结论1:bitmap在预处理后的streamedges没有卵用
		/*if (bitmap==nullptr) {
			for (int i=0;i<partitions;i++) {
				should_access_shard[i] = true;
			}
		} else {
			for (int i=0;i<partitions;i++) {
				should_access_shard[i] = false;
			}
			#pragma omp parallel for schedule(dynamic) num_threads(parallelism)
			for (int partition_id=0;partition_id<partitions;partition_id++) {
				VertexId begin_vid, end_vid;
				std::tie(begin_vid, end_vid) = get_partition_range(vertices, partitions, partition_id);
				VertexId i = begin_vid;
				while (i<end_vid) {
					unsigned long word = bitmap->data[WORD_OFFSET(i)];
					if (word!=0) {
						should_access_shard[partition_id] = true;
						break;
					}
					i = (WORD_OFFSET(i) + 1) << 6;
				}
			}
			#pragma omp barrier
		}*/

		T value = zero;
		Queue<std::tuple<int, long, long, int> > tasks(65536);
		std::vector<std::thread> threads;
		long read_bytes = 0;

		long total_bytes = 0;
		for (int i=0;i<partitions;i++) {
			if (!should_access_shard[i]) continue;//continue 会跳过当前循环中的代码，强迫开始下一次循环。
			for (int j=0;j<partitions;j++) {
				total_bytes += fsize[i][j];    //fsize（物理划分的大小）
			}
		}
		int read_mode;
		if (memory_bytes < total_bytes) {
			read_mode = O_RDONLY | O_DIRECT;
			// printf("use direct I/O\n");
		} else {
			read_mode = O_RDONLY;
			           // 确定printf("use buffered I/O\n");
		}

		int fin;
		long offset = 0;
		long block_size_threads = (block_size/parallelism)/ edge_unit * edge_unit;
		//printf("block_size_threads: %ld\n", block_size_threads);
		switch(update_mode) {
		case 0: // source oriented update（源定向更新（工作准备））     两种遍历block的方式一种行优先一种列优先（这个大概是行优先）
			threads.clear();    //清空线程
			for (int ti=0;ti<parallelism;ti++) {
				threads.emplace_back([&](int thread_id){       //插入一个新的线程
					T local_value = zero;
					long local_read_bytes = 0;
					while (true) {
						int fin, partition_id;
						long offset, length;
						std::tie(fin, offset, length, partition_id) = tasks.pop();
						if (fin==-1) break;
						char * buffer = buffer_pool[thread_id];     //buffer_pool页表池子
						long bytes = pread(fin, buffer, length, offset);          //成功时返回读取的字节数      offset偏移量 读取地址=文件开始+offset。注意，执行后，文件偏移指针不变
						assert(bytes>0);
						local_read_bytes += bytes;
						// CHECK: start position should be offset % edge_unit
						for (long pos=offset % edge_unit;pos+edge_unit<=bytes;pos+=edge_unit) {
							Edge & e = *(Edge*)(buffer+pos);          //读出的边
							//if (bitmap==nullptr || bitmap->get_bit(e.source)) {
								local_value += process_pagerank(e);       //第一个函数的并发执行
							//}
						}
					}
					write_add(&value, local_value);    //确保原子性
					write_add(&read_bytes, local_read_bytes);
				}, ti);
			}
			fin = open((path+"/row").c_str(), read_mode);
			posix_fadvise(fin, 0, 0, POSIX_FADV_SEQUENTIAL);    //清理缓存
			for (int i=0;i<partitions;i++) {
				if (!should_access_shard[i]) continue;
				for (int j=0;j<partitions;j++) {                      //Pji表示第j个作业共享的已加载图形结构分区Pi
					long begin_offset = row_offset[i*partitions+j];
					if (begin_offset - offset >= PAGESIZE) {
						offset = begin_offset / PAGESIZE * PAGESIZE;        
					}
					long end_offset = row_offset[i*partitions+j+1];
					if (end_offset <= offset) continue;
					while (end_offset - offset >= IOSIZE) {
						tasks.push(std::make_tuple(fin, offset, IOSIZE, i));
						offset += IOSIZE;
					}
					if (end_offset > offset) {
						tasks.push(std::make_tuple(fin, offset, (end_offset - offset + PAGESIZE - 1) / PAGESIZE * PAGESIZE, i));
						offset += (end_offset - offset + PAGESIZE - 1) / PAGESIZE * PAGESIZE;
					}
				}
			}
			for (int i=0;i<parallelism;i++) {
				tasks.push(std::make_tuple(-1, 0, 0, -1));
			}
			for (int i=0;i<parallelism;i++) {
				threads[i].join();
			}
			break;
		case 1: // target oriented update     591579114
			fin = open((path+"/column").c_str(), read_mode);
			posix_fadvise(fin, 0, 0, POSIX_FADV_SEQUENTIAL);

			for (int cur_partition=0;cur_partition<partitions;cur_partition+=partition_batch) {
				VertexId begin_vid, end_vid;
				begin_vid = get_partition_range(vertices, partitions, cur_partition).first;
				if (cur_partition+partition_batch>=partitions) {
					end_vid = vertices;
				} else {
					end_vid = get_partition_range(vertices, partitions, cur_partition+partition_batch).first;
				}
				pre_source_window(std::make_pair(begin_vid, end_vid));
				// printf("pre %d %d\n", begin_vid, end_vid);
				threads.clear();
				for (int ti=0;ti<parallelism;ti++) {
					threads.emplace_back([&](int thread_id){
						T local_value = zero;
						long local_read_bytes = 0;
						while (true) {
							int fin, partition_id;
							long offset, length;
							std::tie(fin, offset, length, partition_id) = tasks.pop();
							if (fin==-1) break;
							if (length==-1) break;
							if(chunk_memory==nullptr||chunk_memory->search(offset)==0||length!=chunk_memory->get_chunk_size())
							{
							char * buffer = buffer_pool[thread_id];
							long bytes = pread(fin, buffer, length, offset);
							//assert(bytes>0);
							//printf("bytes:%ld\n",bytes);
							local_read_bytes += bytes;
							// CHECK: start position should be offset % edge_unit
                            long pos;
                                /*if(should_access_shard_pagerank[partition_id])
                                    for(long pos_block = offset % edge_unit ; pos_block+edge_unit<=bytes; pos_block+=edge_unit){
                                        Edge & e = *(Edge*)(buffer+pos_block);

                                        process_pagerank(e);
                                    }
                                if(should_access_shard_sssp[partition_id])
                                    for(long pos_block = offset % edge_unit; pos_block+edge_unit<=bytes; pos_block+=edge_unit){
                                        Edge & e = *(Edge*)(buffer+pos_block);

                                        local_value += process_sssp(e);
                                    }
                                if(should_access_shard_bfs[partition_id])
                                    for(long pos_block = offset % edge_unit; pos_block+edge_unit<=bytes; pos_block+=edge_unit){
                                        Edge & e = *(Edge*)(buffer+pos_block);

                                        local_value += process_bfs(e);
                                    }
                                if(should_access_shard_wcc[partition_id])
                                    for(long pos_block = offset % edge_unit; pos_block+edge_unit<=bytes; pos_block+=edge_unit){
                                        Edge & e = *(Edge*)(buffer+pos_block);

                                        local_value += process_wcc(e);
                                    }*/

							for (pos=offset % edge_unit; pos+block_size_threads<=bytes; pos+=block_size_threads) {
                                if(should_access_shard_pagerank[partition_id])
                                    for(long pos_block = pos; pos_block+edge_unit<=pos+block_size_threads; pos_block+=edge_unit){
                                        Edge & e = *(Edge*)(buffer+pos_block);

                                        process_pagerank(e);
                                    }
                                if(should_access_shard_sssp[partition_id])
                                    for(long pos_block = pos; pos_block+edge_unit<=pos+block_size_threads; pos_block+=edge_unit){
                                        Edge & e = *(Edge*)(buffer+pos_block);

                                        local_value += process_sssp(e);
                                    }
                                if(should_access_shard_bfs[partition_id])
                                    for(long pos_block = pos; pos_block+edge_unit<=pos+block_size_threads; pos_block+=edge_unit){
                                        Edge & e = *(Edge*)(buffer+pos_block);

                                        local_value += process_bfs(e);
                                    }
                                if(should_access_shard_wcc[partition_id])
                                    for(long pos_block = pos; pos_block+edge_unit<=pos+block_size_threads; pos_block+=edge_unit){
                                        Edge & e = *(Edge*)(buffer+pos_block);

                                        local_value += process_wcc(e);
                                    }
							}//最后一个之前的循环处理

							if(pos < bytes){
                                if(should_access_shard_pagerank[partition_id])
                                    for(long pos_block = pos; pos_block+edge_unit<=bytes; pos_block+=edge_unit){
                                        Edge & e = *(Edge*)(buffer+pos_block);

                                        process_pagerank(e);
                                }
                                if(should_access_shard_sssp[partition_id])
                                    for(long pos_block = pos; pos_block+edge_unit<=bytes; pos_block+=edge_unit){
                                        Edge & e = *(Edge*)(buffer+pos_block);

                                        local_value += process_sssp(e);
                                }
                                if(should_access_shard_bfs[partition_id])
                                    for(long pos_block = pos; pos_block+edge_unit<=bytes; pos_block+=edge_unit){
                                        Edge & e = *(Edge*)(buffer+pos_block);

                                        local_value += process_bfs(e);
                                }
                                if(should_access_shard_wcc[partition_id])
                                    for(long pos_block = pos; pos_block+edge_unit<=bytes; pos_block+=edge_unit){
                                        Edge & e = *(Edge*)(buffer+pos_block);

                                        local_value += process_wcc(e);
                                }
							}
						
						    }
							else
							{
								long pos;
								for (pos=offset % edge_unit; pos+block_size_threads<=length; pos+=block_size_threads)
								{
									if(should_access_shard_pagerank[partition_id])
                                    for(long pos_block = pos; pos_block+edge_unit<=pos+block_size_threads; pos_block+=edge_unit){
                                        Edge & e = *(Edge*)(chunk_memory->search(offset)+pos_block);

                                        process_pagerank(e);
                                    }
									if(should_access_shard_sssp[partition_id])
                                    for(long pos_block = pos; pos_block+edge_unit<=pos+block_size_threads; pos_block+=edge_unit){
                                        Edge & e = *(Edge*)(chunk_memory->search(offset)+pos_block);

                                        local_value += process_sssp(e);
                                    }
									if(should_access_shard_bfs[partition_id])
									for(long pos_block = pos; pos_block+edge_unit<=pos+block_size_threads; pos_block+=edge_unit){
                                        Edge & e = *(Edge*)(chunk_memory->search(offset)+pos_block);

                                        local_value += process_bfs(e);
                                    }
									if(should_access_shard_wcc[partition_id])
                                    for(long pos_block = pos; pos_block+edge_unit<=pos+block_size_threads; pos_block+=edge_unit){
                                        Edge & e = *(Edge*)(chunk_memory->search(offset)+pos_block);

                                        local_value += process_wcc(e);
									}
							    }
								
								
								if(pos < length){
                                if(should_access_shard_pagerank[partition_id])
                                    for(long pos_block = pos; pos_block+edge_unit<=length; pos_block+=edge_unit){
                                        Edge & e = *(Edge*)(chunk_memory->search(offset)+pos_block);

                                        process_pagerank(e);
                                }
                                if(should_access_shard_sssp[partition_id])
                                    for(long pos_block = pos; pos_block+edge_unit<=length; pos_block+=edge_unit){
                                        Edge & e = *(Edge*)(chunk_memory->search(offset)+pos_block);

                                        local_value += process_sssp(e);
                                }
                                if(should_access_shard_bfs[partition_id])
                                    for(long pos_block = pos; pos_block+edge_unit<=length; pos_block+=edge_unit){
                                        Edge & e = *(Edge*)(chunk_memory->search(offset)+pos_block);

                                        local_value += process_bfs(e);
                                }
                                if(should_access_shard_wcc[partition_id])
                                    for(long pos_block = pos; pos_block+edge_unit<=length; pos_block+=edge_unit){
                                        Edge & e = *(Edge*)(chunk_memory->search(offset)+pos_block);

                                        local_value += process_wcc(e);
						
									}
								}
						
							}
						
						
						
						
						}
						write_add(&value, local_value);
						write_add(&read_bytes, local_read_bytes);
					}, ti);
				}
				offset = 0;
				for (int j=0;j<partitions;j++) {
					for (int i=cur_partition;i<cur_partition+partition_batch;i++) {
						if (i>=partitions) break;
						if (!should_access_shard[i]) continue;
						long begin_offset = column_offset[j*partitions+i];
						/*if (begin_offset - offset >= PAGESIZE) {
							offset = begin_offset / PAGESIZE * PAGESIZE;
						}*/
						offset = begin_offset;

						long end_offset = column_offset[j*partitions+i+1];
						if (end_offset <= offset) continue;
						while (end_offset - offset >= IOSIZE) {
							tasks.push(std::make_tuple(fin, offset, IOSIZE, i));
							offset += IOSIZE;
						}
						if (end_offset > offset) {
							tasks.push(std::make_tuple(fin, offset, end_offset - offset, i));
							offset += (end_offset - offset);
						}
					}
				}
				for (int i=0;i<parallelism;i++) {
					tasks.push(std::make_tuple(-1, 0, 0, -1));
				}
				for (int i=0;i<parallelism;i++) {
					threads[i].join();
				}
				post_source_window(std::make_pair(begin_vid, end_vid));
				// printf("post %d %d\n", begin_vid, end_vid);
			}

			break;
		default:
			assert(false);
		}

		close(fin);
		// printf("streamed %ld bytes of edges\n", read_bytes);
		return value;
	}
};

#endif
