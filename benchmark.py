import time
import pandas as pd
import os

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
        
        print(f"{num_components} CC // Execution time: {execution_time}s // Iterations: {num_iterations}", flush=True)
        
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
        print(f"Method {method_name} failed with {graph_name}. Error: {str(e)}")
        
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
    log_filename = "lastRunLogs.txt"
    
    # On initialise le fichier de logs (les anciennes exécutions sont écrasées)
    with open(log_filename, "w") as log_file:
        log_file.write(f"--- Benchmark CCF démarré le {time.strftime('%Y-%m-%d %H:%M:%S')} ---\n\n")
    
    for graph in graph_files:
        print(f"Processing graph: {graph['name']}")
        
        rdd = load_graph_to_rdd(sc, graph['path'])
        
        rdd.cache()
        rdd.count()
        
        for method in methods_to_test:
            res = run_experiment(sc, rdd, method, graph['name'])
            results.append(res)
            
            # Écriture immédiate dans le fichier log après chaque itération
            with open(log_filename, "a") as log_file:
                if res["Status"] == "Success":
                    log_file.write(f"[{graph['name']}] Method: {method.ljust(15)} | Status: Success | Time: {res['Time (s)']:<6}s | Iterations: {res['Iterations']:<3} | Components: {res['Components']}\n")
                else:
                    log_file.write(f"[{graph['name']}] Method: {method.ljust(15)} | Status: Failed  | Time: {res['Time (s)']:<6}s\n")
            
        rdd.unpersist() 

    df_results = pd.DataFrame(results)
    print(df_results.to_string(index=False))
    
    df_results.to_csv("benchmark_results.csv", index=False)
    
    return df_results
