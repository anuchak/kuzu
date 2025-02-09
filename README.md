# Robust Recursive Query Parallelism in Graph Database Management System

# Table of Contents
1. [Overview](#Overview)
2. [Build Steps](#Build-Steps)
3. [Executing Queries](#Executing-Queries)
4. [Contact](#Contact)

# Overview

For an overview of our one-time subgraph matching optimizer, check our paper.
We study the problem of optimizing subgraph queries using the new worst-case optimal join plans. Worst-case optimal plans evaluate queries by matching one query vertex 
at a time using multi-way intersections. The core problem in optimizing worst-case optimal plans is to pick an ordering of the query vertices to match. We design a cost-
based optimizer that (i) picks efficient query vertex orderings for worst-case optimal plans; and (ii) generates hybrid plans that mix traditional binary joins with 
worst-case optimal style multiway intersections. Our cost metric combines the cost of binary joins with a new cost metric called intersection-cost. The plan space of our 
optimizer contains plans that are not in the plan spaces based on tree decompositions from prior work.

# Build Steps


    To do a full clean build: ./gradlew clean build installDist
    All subsequent builds: ./gradlew build installDist


# Executing Queries

# Contact

# Kùzu
Kùzu is an embedded graph database built for query speed and scalability. Kùzu is optimized for handling complex join-heavy analytical workloads on very large databases, with the following core feature set:

- Flexible Property Graph Data Model and Cypher query language
- Embeddable, serverless integration into applications 
- Columnar disk-based storage
- Columnar sparse row-based (CSR) adjacency list/join indices
- Vectorized and factorized query processor
- Novel and very fast join algorithms
- Multi-core query parallelism
- Serializable ACID transactions

Kùzu started as a research project at University of Waterloo and is now being 
developed primarily by [Kùzu Inc.](https://kuzudb.com/), a spinoff company from University of Waterloo. 
Kùzu is available under a permissible license. So try it out and help us make it better! We welcome your feedback and feature requests.

## Installation

| Language | Installation                                                           |
| -------- |------------------------------------------------------------------------|
| Python | `pip install kuzu`                                                     |
| NodeJS | `npm install kuzu`                                                     |
| Rust   | `cargo add kuzu`                                                       |
| Java   | [jar file](https://github.com/kuzudb/kuzu/releases/latest)             |
| C/C++  | [precompiled binaries](https://github.com/kuzudb/kuzu/releases/latest) |
| CLI    | [precompiled binaries](https://github.com/kuzudb/kuzu/releases/latest) |

To learn more about installation, see our [Installation](https://docs.kuzudb.com/installation) page.

## Getting Started

Refer to our [Getting Started](https://docs.kuzudb.com/get-started/) page for your first example.

## Build from Source

You can build from source using the instructions provided in the [developer guide](https://docs.kuzudb.com/developer-guide).

## Contributing
We welcome contributions to Kùzu. If you are interested in contributing to Kùzu, please read our [Contributing Guide](CONTRIBUTING.md).

## License
By contributing to Kùzu, you agree that your contributions will be licensed under the [MIT License](LICENSE).

## Citing Kùzu
If you are a researcher and use Kùzu in your work, we encourage you to cite our work.
You can use the following BibTeX citation:
```
@inproceedings{kuzu:cidr,
  author =  {Xiyang Feng and
             Guodong Jin and
             Ziyi Chen and
             Chang Liu and
             Semih Saliho\u{g}lu},
  title={K\`uzu Graph Database Management System},
  booktitle={CIDR},
  year={2023}
}
@misc{kuzu-github,
  author =  {Xiyang Feng and
             Guodong Jin and
             Ziyi Chen and
             Chang Liu and
             Semih Saliho\u{g}lu},
  title = {{K\`uzu Database Management System Source Code}},
  howpublished = {\url{https://github.com/kuzudb/kuzu}},
  month        = nov,
  year         = 2022
}
```

## Contact Us
You can contact us at [contact@kuzudb.com](mailto:contact@kuzudb.com) or [join our Discord community](https://discord.gg/VtX2gw9Rug).
