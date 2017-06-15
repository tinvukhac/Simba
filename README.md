CS 236 Final Project
===========================================
Team members: Dipankar Baisya, Tin Vu <br/><br/>
This project aims to make students get familiar with Simba by using its indexing system then building some spatial query based-on Simba built-in interface.   

How to run this code?
---------------------
All of new implementation were located at `org.apache.spark.examples`. Please clone this source code to your own machine, then copy POIs ("POIs.csv") and trajectories ("trajectories.csv") dataset to "./datasets". Make sure that you already installed SBT tool (http://www.scala-sbt.org/) to build Scala code. 

Go to your project path, then run the "sbt" command to start working with SBT interactive environment.

Type "compile" to compile your code.

Type "run" to start your program. You will have following options:

*[1] org.apache.spark.sql.simba.examples.CS236BenchmarkQueryC*

*[2] org.apache.spark.sql.simba.examples.CS236BenchmarkQueryD*

*[3] org.apache.spark.sql.simba.examples.CS236BuildingRTreeIndex*

*[4] org.apache.spark.sql.simba.examples.CS236QueryA*

*[5] org.apache.spark.sql.simba.examples.CS236QueryB*

*[6] org.apache.spark.sql.simba.examples.CS236QueryC*
 
*[7] org.apache.spark.sql.simba.examples.CS236QueryD*
  

Part 1: R-Tree Indexing
-----------------------
Choose the option [3] from the list above. This program will take 10% of POIs dataset then build an R-Tree index from sample data. After that, it computes MBRs of indexed partitions then dump it to [mbrs.csv](https://github.com/tinvukhac/Simba/blob/master/mbr_plot/mbrs.csv) file. Please run the Python script at [plot_mbr.py](https://github.com/tinvukhac/Simba/blob/master/mbr_plot/plot_mbr.py) to visualize those MBRs. You may need to install some Python libraries like [matplotlib](https://matplotlib.org/) and [shapely](https://github.com/Toblerity/Shapely) (`pip install shapely --no-binary shapely`) to run this script. Our MBRs will be plotted at file [mbrs.png](https://github.com/tinvukhac/Simba/blob/master/mbr_plot/mbrs.png).

Part 2: Building Spatial Query
------------------------------
Choose the options [4],[5],[6],[7] to run query A,B,C and D.

Query results will be stored at "./query_results/"

Part 3: Benchmark
-----------------
Choose option [1],[2] to benchmark the query C and query D.

Benchmark results will be stored at "./benchmark_results/". 

For query C, benchmark result include 4 files which are corresponding to running time with 1,2,3,4 cores. Each file contains 10 line, which is running time in nanoseconds of query C with grid cell size varying from 100-1000m, step 100m.

For query D, benchmark result include 4 files which are corresponding to running time with 1,2,3,4 cores. Each file contains 10 line, which is running time in nanoseconds of query D with radius varying from 10-100m, step 10m. 

End
---

Simba: Spatial In-Memory Big data Analytics
===========================================
**Simba is now shipped as a standalone package outside Spark. Current version works with Spark 2.1.x. If you find any issues, please make a ticket in the issue tracking system.**

Simba is a distributed in-memory spatial analytics engine based on Apache Spark. It extends the Spark SQL engine across the system stack to support rich spatial queries and analytics through both SQL and the DataFrame API. Besides, Simba introduces native indexing support over RDDs in order to develop efficient spatial operators. It also extends Spark SQL's query optimizer with spatial-aware and cost-based optimizations to make the best use of existing indexes and statistics.

Simba is open sourced under Apache License 2.0. Currently, it is developed based on Spark 1.6.0. For recent updates and further information, please refer to [Simba's homepage](http://www.cs.utah.edu/~dongx/simba).

Features
--------------
+ Expressive **SQL and DataFrame query interface** fully *compatible with original Spark SQL operators*. (SQL mode is currently not supported in the standalone version.)
+ Native distributed **indexing** support over RDDs.
+ Efficient **spatial operators**: *high-throughput* & *low-latency*.
    - Box range query: `IN RANGE`
    - Circle range query: `IN CIRCLERANGE`
    - *k* nearest neighbor query: `IN KNN`
    - Distance join: `DISTANCE JOIN`
    - kNN join: `KNN JOIN`
+ Modified Zeppelin: **interactive visualization** for Simba.
+ Spatial-aware **optimizations**: *logical* & *cost-based*.
+ Native thread-pool for multi-threading.
+ **Geometric objects** support (developing)
+ **Spatio-Temporal** and **spatio-textual** data analysis (developing)

**Notes:** *We are still cleaning source codes for some of our features, which will be released to the master and develop branch later.*

Developer Notes
---------------
1. Fork this repo (or create your own branch if you are a member of Simba's main development team) to start your development, **DO NOT** push your draft version to the master branch
2. You can build your own application in `org.apache.spark.examples` package for testing or debugging.
3. If you want to merge your feature branch to the main develop branch, please create a pull request from your local branch to develop branch (**not the master branch**).
4. Use IDE to debug this project. If you use IntelliJ IDEA, [INSTALL](./INSTALL.md) file contains a way to import the whole project to IntelliJ IDEA

Branch Information
------------------
`standalone` branches are opened for maintaining Simba standalone package, which aims at building Simba packages standing outside Spark SQL core. Currently, `master` branch and `develop` branch are built on top of Spark 2.1.x. 

The `master` branch provides the latest stable version, while the `develop` branch is the main development branch where new features will be merged before ready to release. For legacy reasons, we also keep branches which archives old versions of Simba, which is developed based on former Spark versions, in the branches named `simba-spark-x.x`. Note that we will only integrate latest features into `master` and `develop` branches. Please make sure you checkout the correct branch before start using it.

Contributors
------------
- Dong Xie: dongx [at] cs [dot] utah [dot] edu
- Gefei Li: oizz01 [at] sjtu [dot] edu [dot] cn
- Liang Zhou: nichozl [at] sjtu [dot] edu [dot] cn
- Zhongpu Chen: chenzhongpu [at] sjtu [dot] edu [dot] cn
- Feifei Li: lifeifei [at] cs [dot] utah [dot] edu
- Bin Yao: yaobin [at] cs [dot] sjtu [dot] edu [dot] cn
- Minyi Guo: guo-my [at] cs [dot] sjtu [dot] edu [dot] cn
