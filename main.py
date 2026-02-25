import os
from pyspark import SparkConf, SparkContext
from benchmark import run_benchmark
from graph_data import generate_small_world, generate_line_graph

os.environ["SPARK_LOG_DIR"] = "logs"
os.environ["SPARK_CONF_DIR"] = "."
os.environ["PYSPARK_SUBMIT_ARGS"] = "--conf spark.driver.extraJavaOptions=-Dlog4j.configuration=file:log4j.properties pyspark-shell"
os.environ['PYSPARK_PYTHON'] = r".venv\Scripts\python.exe"
os.environ['PYSPARK_DRIVER_PYTHON'] = r".venv\Scripts\python.exe"


import warnings
warnings.filterwarnings("ignore", category=UserWarning) #retire les warnings psutil en particulier, pénibles pour track la progression



def main(data_dir='data'):
    conf = SparkConf() \
        .setAppName("CCF") \
        .setMaster("local[4]") \
        .set("spark.python.worker.faulthandler.enabled", "true") \
        .set("spark.driver.memory", "16g") \
        .set("spark.executor.memory", "4g") \
        .set("spark.python.worker.reuse", "true") \
        .set("spark.sql.execution.arrow.enabled", "false")
         
    
    sc = SparkContext(conf=conf)
    sc.setLogLevel("ERROR")


    generate_small_world(1000, 5000)
    generate_small_world(10000, 50000)
    generate_small_world(100000, 500000)
    generate_line_graph(1000)
    
    graph_files = [
        {"name": "SmallWorld_1000", "path": f"{data_dir}/SmallWorld_1000_5000.txt"},
        #{"name": "SmallWorld_10k", "path": f"{data_dir}/SmallWorld_10000_50000.txt"},
        #{"name": "SmallWorld_100k", "path": f"{data_dir}/SmallWorld_100000_500000.txt"},
        #{"name": "LineGraph_1000", "path": f"{data_dir}/LineGraph_1000.txt"},
        {"name": "Web-Google", "path": f"{data_dir}/web-Google.txt"},
        {"name": "Web-Berkstan", "path": f"{data_dir}/web-BerkStan.txt"},
        {"name": "LiveJournal", "path": f"{data_dir}/soc-LiveJournal1.txt"},
        {"name": "Orkut", "path": f"{data_dir}/com-orkut.ungraph.txt"}
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