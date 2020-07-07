#ifndef GRAPH_H
#define GRAPH_H

#include <stdio.h>
#include <stdlib.h>
#include <math.h>
#include <unistd.h>
#include <malloc.h>
#include <omp.h>
#include <string.h>
#include <map>

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

bool f_true(VertexId v) {
	return true;
}

void f_none_1(std::pair<VertexId,VertexId> vid_range) {

}

void f_none_2(std::pair<VertexId,VertexId> source_vid_range, std::pair<VertexId,VertexId> target_vid_range) {

}

class Graph {
	int parallelism;     //同时执行进程数
	int edge_unit;       //一条边的大小
	bool * should_access_shard;     //活动分区信息
	long ** fsize;                  //各个物理划分的文件大小
	char ** buffer_pool;            //缓存池，有进程数量的块，每块大小为1页
	long * column_offset;             //与下面一个是blocks行排列一个是blocks列排列（123456789）（147258369）
	long * row_offset;
	long memory_bytes;              //RAM大小（假定非常大）
	int partition_batch;
	long vertex_data_bytes;         //节点数据总大小
	
	long PAGESIZE;                  //页大小
public:
	std::string path;

	int edge_type;         //图类型（0无权图，1有权图）
	VertexId vertices;     //节点总数
	EdgeId edges;          //边总数
	int partitions;        //分区数

	int chunk_size;        //chunk表

	Graph (std::string path) {
		PAGESIZE = 4096;
		parallelism = std::thread::hardware_concurrency();
		//parallelism=1;
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
			PAGESIZE = 4096;       //4KB
		} else {
			PAGESIZE = 12288;      //12KB
		}

		should_access_shard = new bool[partitions];            //四个块

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
	}

	Bitmap * alloc_bitmap() {
		return new Bitmap(vertices);
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
					end_vid = vertices;
				} else {
					end_vid = get_partition_range(vertices, partitions, cur_partition+partition_batch).first;
				}
				pre(std::make_pair(begin_vid, end_vid));
				#pragma omp parallel for schedule(dynamic) num_threads(parallelism)
				for (int partition_id=cur_partition;partition_id<cur_partition+partition_batch;partition_id++) {
					if (partition_id < partitions) {
						T local_value = zero;
						VertexId begin_vid, end_vid;
						std::tie(begin_vid, end_vid) = get_partition_range(vertices, partitions, partition_id);
						for (VertexId i=begin_vid;i<end_vid;i++) {
							local_value += process(i);
						}
						write_add(&value, local_value);
					}
				}
				#pragma omp barrier
				post(std::make_pair(begin_vid, end_vid));
			}
		} else {
			#pragma omp parallel for schedule(dynamic) num_threads(parallelism)
			for (int partition_id=0;partition_id<partitions;partition_id++) {
				T local_value = zero;
				VertexId begin_vid, end_vid;
				std::tie(begin_vid, end_vid) = get_partition_range(vertices, partitions, partition_id);
				if (bitmap==nullptr) {
					for (VertexId i=begin_vid;i<end_vid;i++) {
						local_value += process(i);
					}
				} else {
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
		int x = (int)ceil(bytes / (0.8 * memory_bytes));     //celi（向上取整）
		partition_batch = partitions / x;
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

    void set_sizeof_chunk(long cache_size, long graph_size, long specific_size){
	    printf("cache_size: %ld, graph_size: %ld, specific_size: %ld, edge_unit: %ld\n", cache_size, graph_size, specific_size, edge_unit);
        long block_s = cache_size/(1.0+ (specific_size *1.0) / graph_size);
        printf("block_s: %ld\n", block_s);
		printf("原block对边整除: %ld\n",(block_s/edge_unit)*edge_unit);
		printf("原block对页整除: %ld\n",((block_s/edge_unit)*edge_unit)/CHUNKSIZE);
        this->chunk_size = ((((block_s/edge_unit)*edge_unit)/CHUNKSIZE)*CHUNKSIZE)/parallelism;
        printf("block_size: %ld\n", this->chunk_size);
    }

	template <typename T>
	T stream_edges(std::function<T(Edge&)> process, Bitmap * bitmap = nullptr, T zero = 0, int update_mode = 1,
		std::function<void(std::pair<VertexId,VertexId> vid_range)> pre_source_window = f_none_1,
		std::function<void(std::pair<VertexId,VertexId> vid_range)> post_source_window = f_none_1,
		std::function<void(std::pair<VertexId,VertexId> vid_range)> pre_target_window = f_none_1,
		std::function<void(std::pair<VertexId,VertexId> vid_range)> post_target_window = f_none_1) {       //遍历所有边 nullptr能够隐式的转换为任何指针
		if (bitmap==nullptr) {
			for (int i=0;i<partitions;i++) {
				should_access_shard[i] = true;          //某行是否活跃
			}
		} 
		else {
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
		}
        
		T value = zero;
		Queue<std::tuple<int, long, long, int, int, int> > tasks(65536);        //fin（文件指针）    offset偏移量，     pagesize页大小
		std::vector<std::thread> threads;
		long read_bytes = 0;

		long total_bytes = 0;
		for (int i=0;i<partitions;i++) {
			if (!should_access_shard[i]) continue;
			for (int j=0;j<partitions;j++) {
				total_bytes += fsize[i][j];         //文件大小
			}
		}
		int read_mode;
		if (memory_bytes < total_bytes) {
			read_mode = O_RDONLY | O_DIRECT;
			// printf("use direct I/O\n");
		} else {
			read_mode = O_RDONLY;
			// printf("use buffered I/O\n");
		}

		int fin;
		long offset = 0;
		long total_edges = 0;
		int round = 1;
		switch(update_mode) {
		case 0: // source oriented update
			threads.clear();
			for (int ti=0;ti<parallelism;ti++) {
				threads.emplace_back([&](int thread_id){
					T local_value = zero;
					long local_read_bytes = 0;
					while (true) {
						int fin;
						long offset, length;
						int out,in;
						int flags;
						std::tie(fin, offset, length, out, in, flags) = tasks.pop();
						if (fin==-1) break;          //代表先进先出队列已经处理完成
						char * buffer = buffer_pool[thread_id];        //指向buffer池子中进程id对应的buffer
						long bytes = pread(fin, buffer, length, offset);     //原子的读取数据返回值为读取长度
						assert(bytes>0);
						local_read_bytes += bytes;
						// CHECK: start position（起始位置） should be offset % edge_unit（读取出边）
						for (long pos=offset % edge_unit;pos+edge_unit<=bytes;pos+=edge_unit) {
							Edge & e = *(Edge*)(buffer+pos);
							if (bitmap==nullptr || bitmap->get_bit(e.source)) {
								local_value += process(e);
							}
						}
					}
					write_add(&value, local_value);
					write_add(&read_bytes, local_read_bytes);
				}, ti);
			}
			fin = open((path+"/row").c_str(), read_mode);
			posix_fadvise(fin, 0, 0, POSIX_FADV_SEQUENTIAL);
			for (int i=0;i<partitions;i++) {
				if (!should_access_shard[i]) continue;
				for (int j=0;j<partitions;j++) {
					long begin_offset = row_offset[i*partitions+j];
					if (begin_offset - offset >= PAGESIZE) {
						offset = begin_offset / PAGESIZE * PAGESIZE;
					}
					long end_offset = row_offset[i*partitions+j+1];
					if (end_offset <= offset) continue;
					while (end_offset - offset >= IOSIZE) {
						tasks.push(std::make_tuple(fin, offset, IOSIZE, i, j, 0));
						offset += IOSIZE;
					}
					if (end_offset > offset) {
						tasks.push(std::make_tuple(fin, offset, (end_offset - offset + PAGESIZE - 1) / PAGESIZE * PAGESIZE, i, j, 1));
						offset += (end_offset - offset + PAGESIZE - 1) / PAGESIZE * PAGESIZE;
					}
				}
			}
			for (int i=0;i<parallelism;i++) {
				tasks.push(std::make_tuple(-1, 0, 0, -1, -1, -2));
			}
			for (int i=0;i<parallelism;i++) {
				threads[i].join();
			}
			break;
		case 1: // target oriented update
			fin = open((path+"/column").c_str(), read_mode);//开文件
			posix_fadvise(fin, 0, 0, POSIX_FADV_SEQUENTIAL);
			
			for (int cur_partition=0;cur_partition<partitions;cur_partition+=partition_batch) {
				VertexId begin_vid, end_vid;
				begin_vid = get_partition_range(vertices, partitions, cur_partition).first;
				if (cur_partition+partition_batch>=partitions) {
					end_vid = vertices;
				} else {
					end_vid = get_partition_range(vertices, partitions, cur_partition+partition_batch).first;
				}
				pre_source_window(std::make_pair(begin_vid, end_vid));     //预处理1此处无特别效果

				printf("size of each pair is %dB\n", sizeof(std::pair<VertexId, int>));
				printf("pre %d %d\n", begin_vid, end_vid);
				threads.clear();
				
				for (int ti=0;ti<parallelism;ti++) {
					threads.emplace_back([&](int thread_id){
						T local_value = zero;
						long loacl_deges = 0;
						long local_read_bytes = 0;
						int chunk_array = open(("./chunk-table-"+std::to_string(thread_id)).c_str(), O_WRONLY|O_TRUNC|O_CREAT, 0644);
						//int chunk_vnum_array = open(("./chunk-vnum-table-"+std::to_string(thread_id)).c_str(), O_WRONLY|O_TRUNC|O_CREAT, 0644);
                        std::map <VertexId, int> c_table;
                        std::map <VertexId, int>::iterator iter;
                        int edge_num = 0;
						int sub_vnum = 0;
						

						while (true) {
							int fin;
							long offset, length;
							int out, in;
							int flags;
							int id = 1;
							std::tie(fin, offset, length, out, in, flags) = tasks.pop();
							if (fin==-1) 
							{
								write_add(&round,1);
								break;
							}
							char * buffer = buffer_pool[thread_id];
							long bytes = pread(fin, buffer, length, offset);
							assert(bytes>0);
							local_read_bytes += bytes;
							
							// CHECK: start position should be offset % edge_unit
							if(flags == 0)
							{
							for (long pos=offset % edge_unit;pos+edge_unit<=bytes;pos+=edge_unit) {
								Edge & e = *(Edge*)(buffer+pos);

                                iter = c_table.find(e.source);
                                if(iter != c_table.end())
                                    iter-> second ++;
                                else
								{
									c_table.insert(std::pair<VertexId, int> (e.source, 1));
									sub_vnum++;
								}
                                edge_num++;
                                if(edge_num * edge_unit >= chunk_size){
                                    Entry *buffer_c = (Entry *)malloc(sizeof(Entry));
									//int *buffer_d = (int *)malloc(4*sizeof(int));
                                    int i = 0;
									
                                    for(iter = c_table.begin(); iter != c_table.end(); iter ++){
                                        //buffer_c[i].source = iter->first;
                                        //buffer_c[i].number = iter->second;
;
                                        i++;
                                    }
									//buffer_c[0].belong_part_j = out;
									//buffer_c[0].belong_part_i = in;
									buffer_c[0].offset = offset;
									buffer_c[0].range = i;
									buffer_c[0].id = id;
									buffer_c[0].chunk_length=edge_num*edge_unit;
									//buffer_c[0].fin=fin;
									offset +=edge_num*edge_unit;
									//buffer_d[3] = round;
									//iter--;
									//buffer_d[2] = iter->first;
									
									//iter = c_table.begin();
									//buffer_d[1] = iter->first;
									//iter = c_table.end();
									
                                    write(chunk_array, buffer_c, sizeof(Entry));
									id++;
									//write(chunk_vnum_array, buffer_d, 4*sizeof(int));
									write_add(&round,1);
                                    local_value += c_table.size();
                                    loacl_deges += edge_num;
                                    c_table.clear();
                                    edge_num = 0;
									sub_vnum = 0;
                                    free(buffer_c);
									//free(buffer_d);
                                }
							}
							if(edge_num > 0){        //改进后按道理不会进本区
                                Entry *buffer_c = (Entry *)malloc(sizeof(Entry));
								//int *buffer_d = (int *)malloc(4*sizeof(int));
                                int i = 0;
                                for(iter = c_table.begin(); iter != c_table.end(); iter ++){
                                  /*buffer_c[i].source = iter->first;
                                    buffer_c[i].number = iter->second;
									buffer_c[i].belong_part_j = out;
									buffer_c[i].belong_part_i = in;
									buffer_c[i].offset = offset; */
                                    i++;
                                }
								//buffer_c[0].belong_part_j = out;
								//buffer_c[0].belong_part_i = in;
								buffer_c[0].offset = offset;
								buffer_c[0].range = i;
								buffer_c[0].id = id;
								buffer_c[0].chunk_length=edge_num*edge_unit;
								//buffer_c[0].fin=fin;
								offset +=edge_num*edge_unit;
								/*iter--;
								buffer_d[2] = iter->first;
								buffer_d[0] = i;
								iter = c_table.begin();
								buffer_d[1] = iter->first; */
								//iter = c_table.end();
								
                                write(chunk_array, buffer_c, sizeof(Entry));
								id++;
								//write(chunk_vnum_array, buffer_d, 4*sizeof(int));
								write_add(&round,1);
								
                                local_value += c_table.size();
                                loacl_deges += edge_num;
                                c_table.clear();
                                edge_num = 0;
                                free(buffer_c);
								//free(buffer_d);
							}
							}
							else{
								//break;
								for (long pos=offset % edge_unit;pos+edge_unit<=bytes;pos+=edge_unit) {
								Edge & e = *(Edge*)(buffer+pos);

                                iter = c_table.find(e.source);
                                if(iter != c_table.end())
                                    iter-> second ++;
                                else
								{
									c_table.insert(std::pair<VertexId, int> (e.source, 1));
									sub_vnum++;
								}
                                edge_num++;
                                if(edge_num * edge_unit >= chunk_size){
                                    Entry *buffer_c = (Entry *)malloc(sizeof(Entry));
									//int *buffer_d = (int *)malloc(4*sizeof(int));
                                    int i = 0;
									
                                    for(iter = c_table.begin(); iter != c_table.end(); iter ++){
                                       // buffer_c[i].source = iter->first;
                                      //  buffer_c[i].number = iter->second;
										//buffer_c[i].belong_part_j = out;
										//buffer_c[i].belong_part_i = in;
										//buffer_c[i].offset = offset;
                                        i++;
                                    }
									//buffer_c[0].belong_part_j = out;
									//buffer_c[0].belong_part_i = in;
									buffer_c[0].offset = offset;
									buffer_c[0].range = i;
									buffer_c[0].id = id;
									buffer_c[0].chunk_length=edge_num*edge_unit;
									//buffer_c[0].fin=fin;
									offset +=edge_num*edge_unit;
									//iter--;
									//buffer_d[2] = iter->first;
									//buffer_d[0] = i;
									//iter = c_table.begin();
									//buffer_d[1] = iter->first;
									//iter = c_table.end();
									
                                    write(chunk_array, buffer_c, sizeof(Entry));
									id++;
									//write(chunk_vnum_array, buffer_d, 4*sizeof(int));
									//round++;
									write_add(&round,1);
                                    local_value += c_table.size();
                                    loacl_deges += edge_num;
                                    c_table.clear();
                                    edge_num = 0;
									sub_vnum = 0;
                                    //free(buffer_c);
									//free(buffer_d);
                                }
							}
							if(edge_num > 0){
                                Entry *buffer_c = (Entry *)malloc(c_table.size()*sizeof(Entry));
								//int *buffer_d = (int *)malloc(sizeof(int));
                                int i = 0;
                                for(iter = c_table.begin(); iter != c_table.end(); iter ++){
                                    //buffer_c[i].source = iter->first;
                                    //buffer_c[i].number = iter->second;
									//buffer_c[i].belong_part_j = out;
									//buffer_c[i].belong_part_i = in;
									//buffer_c[i].offset = offset;
                                    i++;
                                }
								//iter--;
								//buffer_d[2] = iter->first;
								//buffer_d[0] = i;
								//iter = c_table.begin();
								//buffer_d[1] = iter->first;
								//iter = c_table.end();
								
                               
								//buffer_c[0].belong_part_j = out;
								//buffer_c[0].belong_part_i = in;
								buffer_c[0].offset = offset;
								buffer_c[0].range = i;
								buffer_c[0].id = id;
								buffer_c[0].chunk_length=edge_num*edge_unit;
								//buffer_c[0].fin=fin;
								offset +=edge_num*edge_unit;
								 write(chunk_array, buffer_c, sizeof(Entry));
								//write(chunk_vnum_array, buffer_d, 3*sizeof(int));
								write_add(&round,1);
                                local_value += c_table.size();
                                loacl_deges += edge_num;
                                c_table.clear();
                                edge_num = 0;
                                //free(buffer_c);
								//free(buffer_d);
							}
							}

							
							
						}
						
						
						
						close(chunk_array);
						//close(chunk_vnum_array);
						write_add(&value, local_value);
						write_add(&total_edges, loacl_deges);
						write_add(&read_bytes, local_read_bytes);
					}, ti);
				}
				offset = 0;
				for (int j=0;j<partitions;j++) {
					for (int i=cur_partition;i<cur_partition+partition_batch;i++) {
						if (i>=partitions) break;
						if (!should_access_shard[i]) continue;
						long begin_offset = column_offset[j*partitions+i];
						if (begin_offset - offset >= PAGESIZE) {
							offset = begin_offset / PAGESIZE * PAGESIZE;
						}
						long end_offset = column_offset[j*partitions+i+1];
						if (end_offset <= offset) continue;
						while (end_offset - offset >= IOSIZE) {
							tasks.push(std::make_tuple(fin, offset, IOSIZE, j, i, 0));
							offset += IOSIZE;
						}
						if (end_offset > offset) {
							tasks.push(std::make_tuple(fin, offset, (end_offset - offset + PAGESIZE - 1) / PAGESIZE * PAGESIZE, j, i, 1));
							offset += (end_offset - offset + PAGESIZE - 1) / PAGESIZE * PAGESIZE;
						}
					}
				}
				for (int i=0;i<parallelism;i++) {
					tasks.push(std::make_tuple(-1, 0, 0, -1, -1, -2));
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
		printf("The total number of streamed edges is %ld\n", total_edges);
		// printf("streamed %ld bytes of edges\n", read_bytes);
		return value;
	}
};

#endif
