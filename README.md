# GraphSO
A Structure-aware Storage Optimization for Out-of-Core Concurrent Graph Processing, and a lightweight runtime system which runs in any existing graph processing sytems

# Integrated with existing graph processing systems
Init() call is implemented before the processing. GetActiveChunks() and Repartition() are inserted between successive iterations in the program to get the active chunks and construct the logical partitions before each iteration, respectively. Note that, GetActiveChunks() needs to be placed before Repartition(), thereby identifying the active chunks to guild the graph repartitioning. Schedule() replaces the original graph load operation for efficiently loading the logical partitions to be shared by concurrent jobs.

# Compilation
Compilers supporting basic C++11 features (lambdas, threads, etc.) and OpenMP are required, the other requirements and the compiled method are same as the original systems. Take GridGraph as an example:
```
make
```
# Preprocessing
Before running concurrent applications on a graph, the original graph data needs to be first partitioned into the grid format for GridGraph. To partition the original graph data:
```
./bin/preprocess -i [input path] -o [output path] -v [vertices] -p [partitions] -t [edge type: 0=unweighted, 1=weighted]
```
Then, the graph partitions need to be further logically labeled into chunks. In order to label the graph data, just give the size of the last-level cache and the size of the graph data:
```
./bin/Preprocessing [path]  [cache size in MB] [graph size in MB] [memory budget in GB]
```
For example, we want to divide the grid format [LiveJournal](http://snap.stanford.edu/data/soc-LiveJournal1.html) graph into chunks using a machine with 20M Last-level Cache and 8 GB RAM:
```
./bin/Preprocessing /data/LiveJournal 20 526.38 8
```

# Running Applications
We concurrently submmit the PageRank, WCC, BFS, SSSP to GridGraph-M through the concurrent_jobs application. To concurrently run these applications, just need to give the follwing parameters:
```
./bin/concurrent_jobs [path] [number of submissions] [number of iterations] [start vertex id] [cache size in MB] [graph size in MB] [memory budget in GB]
```
For example, to run 10 iterations of above four algorithms as eight jobs (i.e., submitting the same job twice in succession) on the LiveJournal:
```
./bin/concurrent_jobs /data/LiveJournal 2 10 0 20 526.38 8
```
