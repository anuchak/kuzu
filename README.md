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

# Contact

Contact email address: [a8chakra@uwaterloo.ca](mailto:a8chakra@uwaterloo.ca)
