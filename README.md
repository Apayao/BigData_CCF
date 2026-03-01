# How to run our code

### Files to consider exploring:
- graph_data.py: synthetic graph generation, based on real world models and problematic edge cases mentioned in the paper
- ccf.py: implementation of the algorithms presented in the article (CCF-Dedup, CCF-Iterate vanilla, CCF-Iterate optimised v1 & v2), as well as the main loop to count new pairs
- benchmark.py: proceed with one or several executions of a chosen algorithm (generating sample data + running main loop)
- main.py: run all exeperiments at once (can be long depending on the size of the graphs)

## After cloning the repo

- Install dependencies
```
pip install -r requirements.txt
```

- Run all experiments
```
python main.py
```

