import sys
import os
import operator
import random
import socket
import threading
import datetime
import json
from time import sleep
#The Scheduling Algorithms- RANDOM, RR and LL
def RANDOM(task,workers):
    while True:
        workerLock.acquire()
        #randomly assigns a worker
        choice = random.randint(0,len(workers)-1)
        if workers[choice]['freeSlots']>0:
            workers[choice]['freeSlots']-=1
            mouth1=socket.socket()
            mouth1.connect(('Localhost',workers[choice]['port']))
            msg=json.dumps(task).encode()
            mouth1.send(msg)
            mouth1.close()
            workerLock.release()
            break
        #release if no free slot found, it tries again
        workerLock.release()
        sleep(0.001)
rr=0
def RR(task,workers):
    global rr
    while True:
        workerLock.acquire()
        if workers[rr]['freeSlots']>0:
            workers[rr]['freeSlots']-=1
            mouth2=socket.socket()
            mouth2.connect(('Localhost',workers[rr]['port']))
            msg=json.dumps(task).encode()
            mouth2.send(msg)
            mouth2.close()
            workerLock.release()
            #moves to next worker for the next allocation
            rr=(rr+1)%len(workers)
            break
        #release if there was no free slot in that machine
        workerLock.release()
        #tries for next machine/worker if slot not found
        rr=(rr+1)%len(workers)
        sleep(0.001)
def LL(task,workers):
    while True:
        workerLock.acquire()
        max_i=0
        #search for max free slots by iterating through a loop, store first workers free slots and keep checking for max
        max_free=workers[0]['freeSlots']
        for i in range(1,len(workers)):
            if workers[i]['freeSlots']>max_free:
                max_free=workers[i]['freeSlots']
                max_i=i
        #if condition is to check if the machine has more than 0 slots, if all have 0 freeslots, it sleeps for 1 second
        if max_free:
            workers[max_i]['freeSlots']-=1
            mouth3=socket.socket()
            mouth3.connect(('Localhost',workers[max_i]['port']))
            msg=json.dumps(task).encode()
            mouth3.send(msg)
            mouth3.close()
            workerLock.release()
            break
        #if none of the machines are free, then release lock here
        workerLock.release()
        sleep(1)
#Scheduler function (runs as a separate thread)
def yeetacs(jobs, algo, workers):
    #based on input, calling the required function by using one single name
    algorithm={'RANDOM':RANDOM, 'RR':RR,'LL':LL}
    while True:
        while jobs:
            jobLock.acquire()
            #to check if any task was scheduled in this run, if not, we release the lock separately
            flag=False
            #if there are no jobs in the list, it skips this iteration (waits until more jobs come in)
            if len(list(jobs.keys()))==0:
                jobLock.release()
                sleep(0.01)
                continue
            job=list(jobs.keys())[0]
            for i in range(len(jobs[job]['mapTasks'])):
                #find the first map task that has not been scheduled yet
                if jobs[job]['mapTasks'][i]['scheduled']==False:
                    task={'jobID':jobs[job]['jobID'],'taskID':jobs[job]['mapTasks'][i]['task_id'],'time':jobs[job]['mapTasks'][i]['duration'],'algo':algo}
                    jobs[job]['mapTasks'][i]['scheduled']=True
                    algorithm[algo](task,workers)
                    jobLock.release()
                    #to make sure that reduce doesn't have to be run
                    flag=True
                    break
            #if no map task was scheduled in this iteration and all have been completed
            if not flag and len(jobs[job]['mapTasks'])==0:
                for i in range(len(jobs[job]['reduceTasks'])):
                    #search for the first job that has not be scheduled yet
                    if jobs[job]['reduceTasks'][i]['scheduled']==False:
                        task={'jobID':jobs[job]['jobID'],'taskID':jobs[job]['reduceTasks'][i]['task_id'],'time':jobs[job]['reduceTasks'][i]['duration'],'algo':algo}
                        jobs[job]['reduceTasks'][i]['scheduled']=True
                        algorithm[algo](task,workers)
                        jobLock.release()
                        flag=True
                        break
                if not flag:
                    jobLock.release()  
            elif flag:
                break
            else:
                jobLock.release()
        sleep(0.001)
#function to get job requests from requests.py (runs as a separate thread)
def getJobRequests(jobs,algo):
    ear = socket.socket()
    #from port 5000 as mentioned in the question
    ear.bind(('',5000))
    ear.listen(500)
    while(True):
        connection = ear.accept()
        requests = connection[0].recv(65536).decode()
        arrival_time=datetime.datetime.now()
        job = json.loads(requests)
        jobLock.acquire()
        workerLock.acquire()
        log=open('masterlog.txt','a')
        log.write(str(arrival_time)+',JobArrived,'+job['job_id']+','+algo+'\n')
        log.close()
        workerLock.release()
        jobID = job['job_id']
        jobs[jobID] = {'mapTasks':job["map_tasks"], "reduceTasks":job["reduce_tasks"], 'jobID':jobID}
        for map_task in jobs[jobID]['mapTasks']:
            #add a key-value pair scheduled:False to each map task
            map_task['scheduled']=False
        for reduce_task in jobs[jobID]['reduceTasks']:
            #add a key-value pair scheduled:False to each reduce task
            reduce_task['scheduled']=False
        jobLock.release()
        connection[0].close()
        sleep(0.001)
#Listen to job completion updates by workers (runs as a separate thread)
def getWorkerMessage(jobs,workers,algo):
    ear2=socket.socket()
    #5001 port for listening to workers updates
    ear2.bind(('',5001))
    ear2.listen(256)
    while True:
        connection=ear2.accept()
        msg=connection[0].recv(1024).decode()
        removedTask=json.loads(msg)
        workerLock.acquire()
        #increase the freeslot of the worker that finished the task by 1
        workers[int(removedTask['workerID'])-1]['freeSlots']+=1
        jobLock.acquire()
        #Task_id of the format: JobID+"_"+('M'/'R')+TaskNumber. If M then map task, else reduce task
        if 'M' in removedTask['taskID']:
            #to match the taskID with the remaining tasks' taskID
            for i in range(len(jobs[removedTask['jobID']]['mapTasks'])):
                if jobs[removedTask['jobID']]['mapTasks'][i]['task_id']==removedTask['taskID']:
                    #remove the whole dictionary of task info from the list
                    jobs[removedTask['jobID']]['mapTasks'].pop(i)
                    break
        else:
            #to match the taskID with the remaining tasks' taskID
            for i in range(len(jobs[removedTask['jobID']]['reduceTasks'])):
                if jobs[removedTask['jobID']]['reduceTasks'][i]['task_id']==removedTask['taskID']:
                    #remove the whole dictionary of task info from the list
                    jobs[removedTask['jobID']]['reduceTasks'].pop(i)
                    break
            #if no reduce task remaining, then job is complete, we can remove the dictionary from "jobs"
            if len(jobs[removedTask['jobID']]['reduceTasks'])==0:
                log=open('masterlog.txt','a')
                log.write(str(datetime.datetime.now())+',JobFinished,'+removedTask['jobID']+','+algo+'\n')
                log.close()
                jobs.pop(removedTask['jobID'])
        jobLock.release()
        workerLock.release()
        connection[0].close()
        sleep(0.001)
#Lock to control updation of workers list
workerLock = threading.Lock()
#Lock to control jobs dictionary
jobLock = threading.Lock()
conf_file=sys.argv[1]
algo=sys.argv[2]
config = open(conf_file)
config=json.load(config)
#read all workers and store them in a list called workers
workers = config['workers']
for worker in workers:
    #adding a key-value pair called freeSlots that holds the total number of slots of each worker initially
    worker['freeSlots'] = worker['slots']
jobs={}
thread1 = threading.Thread(target = getJobRequests, args=(jobs,algo))
thread2 = threading.Thread(target = getWorkerMessage, args=(jobs, workers, algo))
thread3 = threading.Thread(target = yeetacs, args=(jobs, algo, workers))
thread1.start()
thread2.start()
thread3.start()
thread1.join()
thread2.join()
thread3.join()