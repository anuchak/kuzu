# Robust Recursive Query Parallelism in Graph Database Management System

# Table of Contents
1. [Overview](#Overview)
2. [Build Steps](#Build-Steps)
3. [Executing Queries](#Executing-Queries)
4. [Contact](#Contact)

# Overview

Efficient multi-core parallel processing of recursive path queries is critical for achieving good performance in graph database management systems (GDBMSs). Prior work 
adopts two broad approaches. First is the state of the art morsel-driven parallelism, whose vanilla application in GDBMSs parallelizes computations at the source node 
level. Second is to parallelize each iteration of the computation at the frontier level.

We show that these approaches can be seen as part of a design space of morsel dispatching policies based on picking different granularities of morsels. We then 
empirically study the question of which policies parallelize better in practice under a variety of datasets and query workloads that contain one to
over hundreds of source nodes. We show that the policy of issuing source morsels alone can limit parallelism, as it can assign the entire
work of some queries to a single thread. On the other hand, issuing only frontier-level morsels limits parallelism due to sparse frontiers
in computations. We show that these two policies can be combined in a hybrid policy that issues morsels both at the source node and frontier levels. 

We then show that the multi-source breadth-first search optimization from prior work can also be modeled as a morsel dispatching policy that packs multiple source nodes 
into multi-source morsels. We implement these policies inside a single system, the Kùzu GDBMS, and evaluate them both within Kùzu and across other systems. We show that 
the hybrid policy captures the behavior of both source morsel-only and frontier morsel-only policies in cases when these approaches parallelize well, and outperform them 
on queries when they are limited, and propose it as a robust approach to parallelizing recursive queries. We further show that assigning multi-sources is beneficial, as 
it reduces the amount of scans, but only when there is enough sources in the query.

# Build Steps

release: make clean release NUM_THREADS=32  

# Executing Queries

## Getting Started

After building, run the following command in the project root directory:


You can now move into the scripts folder to load a dataset and execute queries:

cd scripts

## Dataset Preperation

A dataset may consist of two files: (i) a vertex file, where IDs are from 0 to N and each line is of the format (ID,LABEL); and (ii) an edge file where each line is of the format (FROM,TO,LABEL). If the vertex file is omitted, all vertices are assigned the same label. We mainly used datasets from SNAP. The serialize_dataset.py script lets you load datasets from csv files and serialize them to the appropriate format for quick subsequent loading.

To load and serialize a dataset from a single edges files, run the following command in the scripts folder:

python3 serialize_dataset.py /absolute/path/edges.csv /absolute/path/data

The system will assume that all vertices have the same label in this case. The serialized graph will be stored in the data directory. If the dataset consists of an edges file and a vertices file, the following command can be used instead:

python3 serialize_dataset.py /absolute/path/edges.csv /absolute/path/data -v /absolute/path/vertices.csv

After running one of the commands above, a catalog can be generated for the optimizer using the serialize_catalog.py script.

python3 serialize_catalog.py /absolute/path/data  

## Executing Queries

Once a dataset has been prepared, executing a query is as follows:

python3 execute_query.py "(a)->(b),(b)->(c),(c)->(d)" /absolute/path/data

An output example on the dataset of Amazon0601 from SNAP with 1 edge label and 1 verte label is shown below. The dataset loading time, the opimizer run time, the quey execution run time and the query plan with the number of output and intermediate tuples are logged.

Dataset loading run time: 626.713398 (ms)
Optimizer run time: 9.745375 (ms)
Plan initialization before exec run time: 9.745375 (ms)
Query execution run time: 2334.2977 (ms)
Number output tuples: 118175329
Number intermediate tuples: 34971362
Plan: SCAN (a)->(c), Single-Edge-Extend TO (b) From (a[Fwd]), Multi-Edge-Extend TO (d) From (b[Fwd]-c[Fwd])

In order to invoke a multi-threaded execution, one can execute the query above with the following command to use 2 threads.

python3 execute_query.py "(a)->(b),(b)->(c),(c)->(d)" /absolute/path/data -t 2

The query above assigns an arbitrary edge and vertex labels to (a), (b), (c), (a)->(b), and (b)->(c). Use it with unlabeled datasets only. When the dataset has labels, assign labels to each vertex and edge as follows:

python3 execute_query.py "(a:person)-[friendof]->(b:person), (b:person)-[likes]->(c:movie)" /absolute/path/data

Requiring More Memory

Note that the JVM heap by default is allocated a max of 2GB of memory. Changing the JVM heap maximum size can be done by prepending JAVA_OPTS='-Xmx500G' when calling the python scripts:

JAVA_OPTS='-Xmx500G' python3 serialize_catalog.py /absolute/path/data  


# Contact

Contact email address: [a8chakra@uwaterloo.ca](mailto:a8chakra@uwaterloo.ca)
