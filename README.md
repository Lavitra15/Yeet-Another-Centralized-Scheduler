# Yeet-Another-Centralized-Scheduler

This repo is for the 5th Semester Final Project for the course - Big Data (UE18CS322).

# Introduction 
Yeet Another Centralized Scheduler (christened with the same acronym 'YACS') consists of a central Master which is used to schedule 'jobs' which consist of several map and reduce tasks on multiple workers. Every worker has a certain number of 'slots' (compute available on the machine). Each slot can execute one task at a time (either a map task or a reduce task). The number of slots is different for each worker and is specified in the `config.json` provided. Although there is no priority between the map and reduce tasks themselves, the reduce tasks can only start execution after all the map tasks have finished execution.

# Description 
* master.py 
The Master listens to job requests from port 5000 and receives updates from the workers via port 5001. It has three threads.
![Master Threads](https://github.com/Ojjie/Yeet-Another-Centralized-Scheduler/blob/init/Output/MasterFlowchart.png)

* worker.py
The Worker file is used to send job requests and updates to the master via port 5000 and 5001 respectively.
![Worker Threads](https://github.com/Ojjie/Yeet-Another-Centralized-Scheduler/blob/init/Output/WorkerFlowchart.png)

* requests.py
It is used to generate 'n' requests with random number of map and reduce tasks for each job (this can be made a certain number as is done in the `request_eval.py` file.)

Format of request message:
```json
{	"job_id":<job_id>,	
	"map_tasks":[
		{"task_id":"<task_id>","duration":<in seconds>},	
		{"task_id":"<task_id>","duration":<in seconds>}	
		...	
	],	
	"reduce_tasks":[	
		{"task_id":"<task_id>","duration":<in seconds>},	
		{"task_id":"<task_id>","duration":<in seconds>}
		...	
	]
}
```

Format of config.json :
```json
{
	 "Workers": [ //one worker per machine
	    {
		      "worker_id": <worker_id>,
		      "slots": <number of slots>, // number of slots in the machine
		      "port": <port number> // port on which the Worker process listens for task launch messages
	    },
	    {
		      ”worker_id": <worker_id>,
		      "slots": <number of slots>,
		      "port": <port number>
	    },
   	   …
	]
}

```

# Scheduling Algorithms Implemented - 
* Random Selection 
* Round Robin 
* Least Loaded


# Dependencies - 
The following modules need to be imported for the program to run - 
>sys

>os

>operator

>random

>socket 

>threading 

>datetime

>json

>time

>pandas

>matplotlib

>seaborn

>statistics


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

# Output Graphs for Task 2 -
Implemented in `analysis.py` 

## Bar plot of Tasks vs Execution Time for the Scheduling Algorithms 

### Random Selection 
![Random](https://github.com/Ojjie/Yeet-Another-Centralized-Scheduler/blob/init/Output/Task_random.jpeg)
### Round Robin 
![RR](https://github.com/Ojjie/Yeet-Another-Centralized-Scheduler/blob/init/Output/Task_RR.jpeg)
### Least Loaded 
![LL](https://github.com/Ojjie/Yeet-Another-Centralized-Scheduler/blob/init/Output/Task_LL.jpeg)

## Mean and Median execution times observed - 
![Stats](https://github.com/Ojjie/Yeet-Another-Centralized-Scheduler/blob/init/Output/task_exec.png)

## Bar plot of Jobs vs Execution Time for the Scheduling Algorithms

### Random Selection 
![Random](https://github.com/Ojjie/Yeet-Another-Centralized-Scheduler/blob/init/Output/Job_random.jpeg)
### Round Robin 
![RR](https://github.com/Ojjie/Yeet-Another-Centralized-Scheduler/blob/init/Output/Job_RR.jpeg)
### Least Loaded 
![LL](https://github.com/Ojjie/Yeet-Another-Centralized-Scheduler/blob/init/Output/Job_LL.jpeg)

## Mean and Median execution times observed - 
![Stats](https://github.com/Ojjie/Yeet-Another-Centralized-Scheduler/blob/init/Output/job_exec.png)

## Heatmaps of tasks executed vs time for the Scheduling Algorithms 

### Random Selection 
![Random](https://github.com/Ojjie/Yeet-Another-Centralized-Scheduler/blob/init/Output/HeatMap_random.jpeg)
### Round Robin 
![RR](https://github.com/Ojjie/Yeet-Another-Centralized-Scheduler/blob/init/Output/HeatMap_RR.jpeg)
### Least Loaded 
![LL](https://github.com/Ojjie/Yeet-Another-Centralized-Scheduler/blob/init/Output/HeatMap_LL.jpeg)

This project was done by - 

# Team Members:
1. [Tejas Srinivasan](https://github.com/Ojjie) PES1201800110
2. [Arjun Chengappa](https://github.com/arjunchengappa) PES1201800119
3. [Lavitra Kshitij Madan](https://github.com/Lavitra15) PES1201800137
4. [Priyanka Gopi](https://github.com/Priyankagopi86) PES1201801797

`PS - A more detailed explanation and a description of the resources used and the challenges faced while doing this project can be found in the final project report in the same repository.` 
