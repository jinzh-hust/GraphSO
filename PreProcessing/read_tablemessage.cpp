#include "core/Table_memory.hpp"

using namespace std;
  
int main(int argc, char** argv)
{
	string path(argv[1]);
	cout << "read from " << path << endl;
	Table_memory table_memory;
	table_memory.init(4980736,9251455,4);
	int num;
	int offset;
	int belong_part_j;
	int belong_part_i;
	
   for(int i=900;i<1000;i++)
{
std::tie(num, offset, belong_part_j, belong_part_i)=table_memory.get_VertexId_message(i);
	cout << num << " " << offset << " " << belong_part_j << " " << belong_part_i << " "<< endl;
}
   // table_memory.~Table_memory();
	return 0;
}
