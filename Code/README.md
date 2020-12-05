 # Steps to execute - 

- run master.py 
```sh 
        python3 master.py </path/to/config.json> <scheduling_algo(RANDOM/RR/LL)> 
```
- run worker.py in as many terminals(consoles) as there are workers
```sh
        python3 worker.py <port> <worker_id>
```
- run requests.py with the number of requests needed
```sh
        python3 requests.py <no_of_requests>
```
or alternatively run (for evaluation purposes only)-
- run requests_eval.py with the number of requests needed
```sh
        python3 requests_eval.py <no_of_requests>
```

After this is done, to get the outputs for task 2 
- run analysis.py for the masterlog.txt and workerlog.txt files generated from Task 1

```sh
        python3 analysis.py 
```

