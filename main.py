import os
from pyspark import SparkConf, SparkContext
from benchmark import run_benchmark
from graph_data import generate_small_world, generate_line_graph

def main():
    conf = SparkConf() \
        .setAppName("CCF") \
        .setMaster("local[*]") \
        .set("spark.driver.memory", "8g")
    
    sc = SparkContext(conf=conf)
    sc.setLogLevel("ERROR") 


    generate_small_world(100, 500)
    generate_small_world(10000, 50000)
    generate_small_world(100000, 500000)
    generate_line_graph(1000)
    
    graph_files = [
        {"name": "SmallWorld_100", "path": "SmallWorld_100_500.txt"},
        {"name": "SmallWorld_10k", "path": "SmallWorld_10000_50000.txt"},
        {"name": "SmallWorld_100k", "path": "SmallWorld_100000_500000.txt"},
        {"name": "LineGraph_1000", "path": "LineGraph_1000.txt"},
        {"name": "Web-Google", "path": "web-Google.txt"},
        {"name": "Web-Google", "path": "web-BerkStan.txt"}
    ]

    try:
        run_benchmark(sc, graph_files)
    except Exception as e:
        print(f"\nError : {e}")
    finally:
        sc.stop()

if __name__ == "__main__":
    print("Running main")
    main()
    print("Spark context closed")