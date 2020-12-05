import matplotlib.pyplot as plt 
import datetime 
from time import sleep
import seaborn as sns
import statistics
import pandas as pd
from math import ceil
algos=['RANDOM','RR','LL']
#Creating the datastructures 
x_axis = {}
y_axis = {}
times = {}
tasks={}
#Initialising them
for algo in algos:
    x_axis[algo]=[]
    y_axis[algo]=[]
    times[algo]={}
    tasks[algo]={}
    #tasks dictionary of the form   {"RANDOM":{worker1: [...], worker2: [...]... }, "RR":{...}, "LL":{...} }
# Analysis of Task completion time
f1 = open("workerlog.txt","r")
lines = f1.readlines()
f1.close()
#Check the indices once according to the output log file of the worker
for line in lines:
    log_split = line.split(",")  #worker_id, timestamp, message, jobID, taskID, timeLeft, algo
    algo=log_split[6].strip()
    date_time = datetime.datetime.strptime(log_split[1], '%Y-%m-%d %H:%M:%S.%f')
    task_id=log_split[4]
    if log_split[2] == "TaskArrived":
        times[algo][task_id]=[]
        times[algo][task_id].append(date_time)
        #Part2- storing task arrived times per algo
        if log_split[0] not in tasks[algo].keys():
            tasks[algo][log_split[0]]=[]
        tasks[algo][log_split[0]].append(date_time)
    elif log_split[2] == "TaskFinished":
        times[algo][task_id].append(date_time)
    #only if both TaskArrived and TaskFinished
    if len(times[algo][task_id])==2:
        time = times[algo][task_id][1] - times[algo][task_id][0]
        x_axis[algo].append(str(task_id))
        y_axis[algo].append(time.total_seconds())
#plotting the graphs for each algorithm
for algo in algos:
    if len(x_axis[algo])==0:
        continue
    fig = plt.figure()
    plt.xticks(rotation='vertical')
    plt.bar(x_axis[algo],y_axis[algo])
    plt.xlabel('Task ID')
    plt.ylabel('Time in seconds')
    plt.title('Task execution times for '+algo+' algorithm')
    plt.show()
    print('Metrics for task execution time using '+algo+' algorithm')
    print("\tMean:",statistics.mean(y_axis[algo]),'s')
    print("\tMedian:",statistics.median(y_axis[algo]),'s') 
print()
#Analysis of Job completion time 
for algo in algos:
    x_axis[algo]=[]
    y_axis[algo]=[]
    times[algo]={}
f2 = open("masterlog.txt","r")
lines2 = f2.readlines()
f2.close()
for line in lines2:
    log_split = line.split(",")  #Timestamp, message, JobID, algo
    date_time = datetime.datetime.strptime(log_split[0],'%Y-%m-%d %H:%M:%S.%f')
    job_id=log_split[2]
    algo=log_split[3].strip()
    if(log_split[1] == "JobArrived"):
        times[algo][job_id]=[]
        times[algo][job_id].append(date_time)
    if(log_split[1] == "JobFinished"):
        times[algo][job_id].append(date_time)
    #only if both JobArrived and JobFinished
    if len(times[algo][job_id])==2:
        time = times[algo][job_id][1] - times[algo][job_id][0]
        x_axis[algo].append(str(job_id))
        y_axis[algo].append(time.total_seconds())
#plotting the graphs for each algorithm (job completion)
for algo in algos:
    if len(x_axis[algo])==0:
        continue
    fig = plt.figure()
    plt.xticks(rotation='vertical')
    plt.bar(x_axis[algo],y_axis[algo])
    plt.xlabel('Job ID')
    plt.ylabel('Time in seconds')
    plt.title('Job execution times for '+algo+' algorithm')
    plt.show()
    print('Metrics for job execution time using '+algo+' algorithm')
    print("\tMean:",statistics.mean(y_axis[algo]),'s')
    print("\tMedian:",statistics.median(y_axis[algo]),'s') 
#Task 2 result 2
#graphs for number of tasks scheduled in each of the algorithms
time_tasks={}
min_max_times={}
for algo in algos:
    time_tasks[algo]={}
    #min_max_times dictionary to store the first ever task and last ever task arrived foreach algorithm
    min_max_times[algo]={}
    #iterating through all the workers to find the first and last task arrival time
    for i, worker in enumerate(tasks[algo].keys()):
        if i==0:
           first_task=min(tasks[algo][worker])
           continue
        first_task=min(first_task, min(tasks[algo][worker]))
        last_task=max(first_task, max(tasks[algo][worker]))
    #Time range for plotting heatmap
    time_range=ceil((last_task-first_task).total_seconds())
    min_max_times[algo]['FirstTask']=first_task
    min_max_times[algo]['LastTask']=last_task
    min_max_times[algo]['TimeRange']=time_range
    #Initializing the time_task dictionary
    for worker in tasks[algo].keys():
        time_tasks[algo][worker]={}
        for i in range(1,time_range+1):
            time_tasks[algo][worker][i]=0
for line in lines:
    log_split = line.split(",")  #worker_id, timestamp, message, jobID, taskID, timeLeft, algo
    if log_split[2]=='TaskArrived':
        for i in range(1,min_max_times[log_split[6].strip()]['TimeRange']+1):
            date_time = datetime.datetime.strptime(log_split[1],'%Y-%m-%d %H:%M:%S.%f')
            #find which interval (of 1 second length) the task arrival time falls in 
            if (i-1) <= (date_time-min_max_times[log_split[6].strip()]['FirstTask']).total_seconds() < i:
                time_tasks[log_split[6].strip()][log_split[0]][i]+=1
#plotting the heatmaps
for algo in algos:
    df=pd.DataFrame.from_dict(time_tasks[algo])
    if df.shape!=(0,0):
        sns.heatmap(df, linewidth= .5, annot=True)
        plt.title('Number of tasks scheduled on each machine against time for '+algo+' algorithm')
        plt.xlabel('Worker ID')
        plt.ylabel('Time in seconds')
        plt.show()