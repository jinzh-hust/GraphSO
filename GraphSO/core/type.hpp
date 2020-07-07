#ifndef TYPE_H
#define TYPE_H

typedef int VertexId;
typedef long EdgeId;
typedef float Weight;

struct Edge {
	VertexId source;
	VertexId target;
	Weight weight;
};

struct Entry {
	//VertexId source;
	//int number;
	//int belong_part_j;      //属于某块分区(stream_edge case1 外侧循环)
	//int belong_part_i;      //属于某块分区(stream_edge case1 内侧循环)
	long offset;               //partion分区文件的偏移量（定位chunk）
	int range;
	int id;     //该文件量下的第x块
	int chunk_length;
	//int fin;
};

struct MergeStatus {
  int id;
  long begin_offset;
  long end_offset;
};

#endif
