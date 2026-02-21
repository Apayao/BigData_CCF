import os
from pyspark import SparkConf, SparkContext
from benchmark import run_benchmark
from graph_data import generate_small_world, generate_line_graph

def main(data_dir='data'):
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
        {"name": "SmallWorld_100", "path": f"{data_dir}/SmallWorld_100_500.txt"},
        {"name": "SmallWorld_10k", "path": f"{data_dir}/SmallWorld_10000_50000.txt"},
        {"name": "SmallWorld_100k", "path": f"{data_dir}/SmallWorld_100000_500000.txt"},
        {"name": "LineGraph_1000", "path": f"{data_dir}/LineGraph_1000.txt"},
        {"name": "Web-Google", "path": f"{data_dir}/web-Google.txt"},
        {"name": "Web-Google", "path": f"{data_dir}/web-BerkStan.txt"}
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