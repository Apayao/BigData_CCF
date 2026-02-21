import time
import pandas as pd

from ccf import ccf
from graph_data import load_graph_to_rdd

def run_experiment(sc, rdd, method_name, graph_name):

    print(f"\nRunning experiment : {graph_name} | Method : {method_name}")
    
    start_time = time.time()
    
    try:
        final_rdd, num_iterations = ccf(sc, rdd, method=method_name)
        num_components = final_rdd.values().distinct().count()
        
        end_time = time.time()
        execution_time = round(end_time - start_time, 2)
        
        print(f"{num_components} CC // Execution time: {execution_time}s // Iterations: {num_iterations}")
        
        return {
            "Graph": graph_name,
            "Method": method_name,
            "Status": "Success",
            "Time (s)": execution_time,
            "Iterations": num_iterations,
            "Components": num_components
        }

    except Exception as e:
        end_time = time.time()
        print(f"Method {method_name} failed with {graph_name}. Error : {str(e)}")
        
        return {
            "Graph": graph_name,
            "Method": method_name,
            "Status": "Failed",
            "Time (s)": round(end_time - start_time, 2),
            "Iterations": None,
            "Components": None
        }

def run_benchmark(sc, graph_files):

    methods_to_test = ["vanilla", "sec_sort_naive", "optimised"]
    results = []
    
    for graph in graph_files:
        print(f"Processing graph: {graph['name']}")
        
        rdd = load_graph_to_rdd(sc, graph['path'])
        
        rdd.cache()
        rdd.count()
        
        for method in methods_to_test:
            res = run_experiment(sc, rdd, method, graph['name'])
            results.append(res)
            
        rdd.unpersist() 

    df_results = pd.DataFrame(results)
    print(df_results.to_string(index=False))
    
    df_results.to_csv("benchmark_results.csv", index=False)
    
    return df_results