#Making the necessary imports 
import matplotlib.pyplot as plt 
import datetime 
import time 
import seaborn 
import statistics

#Creating the datastructures 
x_axis = []
y_axis = []

exec_time = dict()
start_time = dict()
end_time = dict()


# Analysis of Task completion time 
for i in range(1,4):
    f1 = open(r"log/worker_" + str(i) + ".txt","r"))
    lines = f1.readlines()

#Check the indices once according to the output log file of the worker
    for line in lines:
        timestamp_split = lines.split("\t")
        timestamp_string = timestamp_split[1][0:-1]
        dt = datetime.datetime.strptime(timestamp_string,'%Y-%m-%d %H:%M:%S.%f')
        #print(dt)

        message_split = timestamp_split[2].split("=") #format of message takes '=' sign ?
        message_split[0] = message_split[0].strip()
        message_split[1] = message_split[1][0:-1].strip()
        message_split[1] = message_split[1][1:]
        #print(message_split)

        #Get the worker id 
        if ((message_split[0] == "TaskArrived") or (message_split[0] == "TaskFinished")):
            job_split = message_split[1].split(',')           #jt_split stands for JOB_TASK_SPLIT
            task_split = job_split[1].split(':')       #ti_split stands for TASK_ID_SPLIT
            task_id = task_split[1].split(']')[0]
            #print(task_id)
            if (message_split[0] == "TaskArrived"):
                receive_time = dt
                #print(receive_time)
                start_time[task_id] = receive_time
            if (message_split[0] == "TaskFinished"):
                finish_time = dt
                #print(finish_time)  
                end_time[task_id] = finish_time
            if ((task_id in start_time) and (task_id in end_time)):
                execution_time[task_id] = end_time[task_id] - start_time[task_id]
                #print(task_id, execution_time[task_id].total_seconds())
                x_axis.append(str(task_id))
                y_axis.append(exec_time[task_id].total_seconds())

    
f1.close()

#plotting the graphs 
fig = plt.figure()
ax = fig.add_axes([0,0,1,1])
ax.bar(x_axis,y_axis)
plt.xticks(rotation='vertical')
ax.set_xlabel('taskID')
ax.set_ylabel('Time in seconds')
plt.title('Task execution times')

plt.show()

#need to convert to list before taking mean and median?
print("Mean time of execution of tasks is:",statistics.mean(y_axis),'s')
print("Median time of execution of tasks is:",statistics.median(y_axis),'s')

#Analysis of Job completion time 

x_axis = []
y_axis = []

arrival_time = dict()
completion_time = dict()
job_execution_time = dict()

f2 = open(r"log/master.txt","r")
lines = f2.readlines()

for line in lines:
    timestamp_split = line.split("\t")
    timestamp_string = timestamp_split[1][0:-1]
    dt = datetime.datetime.strptime(timestamp_string,'%Y-%m-%d %H:%M:%S.%f')
    #print(dt)

    message_split = timestamp_split[2].split("=") #format of message takes '=' sign ?
    message_split[0] = message_split[0].strip()
    message_split[1] = message_split[1][0:-1].strip()
    message_split[1] = message_split[1][1:]
    #print(message_split)


    #Get the job id 
    if ((message_split[0] == "TaskArrived") or (message_split[0] == "TaskFinished")):
        if(message_split[0] == "TaskArrived"):
            job_split = message_split[1].split(',')           #jt_split stands for JOB_TASK_SPLIT
            jobid_split = job_split[1].split(':')       #ti_split stands for TASK_ID_SPLIT
            job_id = jobid_split[1]
            receive_time = dt
            #print(receive_time)
            incoming_time[job_id] = receive_time
        if(message_split[0] == "TaskFinished"):
            job_split = message_split
            jobid_split = job_split[1].split(':')
            job_id = jobid_split[1].split(']')[0]
            finish_time = dt   
            #print(finish_time)
            completion_time[job_id] = finish_time
        
        if(job_id in incoming_time and job_id in completion_time):
            job_execution_time[job_id] = completion_time[job_id] - execution_time[job_id]
            x_axis.append(job_id)
            y_axis.append(job_execution_time[job_id].total_seconds())
                
f2.close()

#plotting the graphs 
fig = plt.figure()
ax = fig.add_axes([0,0,1,1])
ax.bar(x_axis,y_axis)
plt.xticks(rotation='vertical')
ax.set_xlabel('jobID')
ax.set_ylabel('Time in seconds')
plt.title('Job execution times')

plt.show()

#need to convert to list before taking mean and median?
print("Mean time of execution of jobs is:",statistics.mean(y_axis),'s')
print("Median time of execution of jobs is:",statistics.median(y_axis),'s')
