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
f1 = open("workerlog.txt","r")
lines = f1.readlines()

#Check the indices once according to the output log file of the worker
    
for line in lines:
    log_split = line.split(",")
    timestamp_string = log_split[1] #[0:-1]
    dt = datetime.datetime.strptime(timestamp_string,'%Y-%m-%d %H:%M:%S.%f')
    #print(dt)

    message_split = log_split[2]

    #print(message_split)

    #Get the worker id 
    if ((message_split == "TaskArrived") or (message_split == "TaskFinished")):
        job_split = log_split[3]          
        task_split = log_split[4]       
        task_id = task_split.split('_')[1]
        #print(task_id)
        if (message_split == "TaskArrived"):
            receive_time = dt
            print(receive_time)
            start_time[task_id] = receive_time
        if (message_split == "TaskFinished"):
            finish_time = dt
            print(finish_time)  
            end_time[task_id] = finish_time
        if ((task_id in start_time) and (task_id in end_time)):
            exec_time[task_id] = end_time[task_id] - start_time[task_id]
            print(task_id, exec_time[task_id].total_seconds())
            x_axis.append(str(task_id))
            y_axis.append(exec_time[task_id].total_seconds())

f1.close()

#plotting the graphs 
fig = plt.figure()
print("check")
ax = fig.add_axes([0,0,100,100])
print("check")
ax.bar(x_axis,y_axis)
print("check")
plt.xticks(rotation='vertical')
print("check")
ax.set_xlabel('taskID')
print("check")
ax.set_ylabel('Time in seconds')
print("check")
plt.title('Task execution times')
print("check")

plt.show()
print("check")

print("Mean time of execution of tasks is:",statistics.mean(y_axis),'s')
print("Median time of execution of tasks is:",statistics.median(y_axis),'s')


#Analysis of Job completion time 

x_axis = []
y_axis = []

arrival_time = dict()
completion_time = dict()
job_execution_time = dict()

f2 = open("masterlog.txt","r")
lines = f2.readlines()

for line in lines:
    log_split = line.split(",")
    timestamp_string = log_split[0]
    dt = datetime.datetime.strptime(timestamp_string,'%Y-%m-%d %H:%M:%S.%f')
    #print(dt)

    message_split = log_split[1]
    
    #print(message_split)


    #Get the job id 
    if ((message_split == "JobArrived") or (message_split == "JobFinished")):
        if(message_split == "JobArrived"):
            job_split = log_split[2]        
            receive_time = dt
            #print(receive_time)
            arrival_time[job_split] = receive_time
        if(message_split == "JobFinished"):
            job_split = log_split[2]
            finish_time = dt   
            #print(finish_time)
            completion_time[job_split] = finish_time
        
        if(job_split in arrival_time and job_split in completion_time):
            job_execution_time[job_split] = completion_time[job_split] - job_execution_time[job_split]
            x_axis.append(job_split)
            y_axis.append(job_execution_time[job_split].total_seconds())
                
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

#Task 2 part 2 
#graphs for number of tasks scheduled in each of the algorithms 

