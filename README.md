# CCF in PySpark: Fast and Scalable Connected Components

## About the Project
This repository provides a PySpark implementation of the algorithms detailed in the paper *"CCF: Fast and Scalable Connected Component Computation in MapReduce"* (Kardes et al.). 

The goal of this project is to efficiently compute connected components in massive graphs using the Min-ID propagation strategy. We explore and compare multiple implementations—from a naive MapReduce translation to highly optimized Spark-native versions that mimic Hadoop's Secondary Sorting (using `repartitionAndSortWithinPartitions`). The project evaluates these approaches on both synthetic topologies and real-world datasets to analyze their limits regarding memory consumption (RAM) and network serialization (Shuffles).

## How to run our code

### Files to consider exploring:
- `graph_data.py`: Synthetic graph generation, based on real-world models and problematic edge cases mentioned in the paper.
- `ccf.py`: Implementation of the algorithms presented in the article (CCF-Dedup, CCF-Iterate vanilla, CCF-Iterate optimised v1 & v2), as well as the main loop to count new pairs.
- `benchmark.py`: Proceed with one or several executions of a chosen algorithm (generating sample data + running main loop).
- `main.py`: Run all experiments at once (can be long depending on the size of the graphs).

## After cloning the repo

1. Install dependencies:
```bash
pip install -r requirements.txt
```
2. Run all experiments:
```bash
python main.py
```

