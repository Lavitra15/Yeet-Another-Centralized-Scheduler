import sys 
import os 
import operator
import random
import socket
import threading
import datetime
import json
from time import sleep

workerLock = threading.Lock()
jobLock = threading.Lock()

def RANDOM(task,workers,algo):
    while True:
        workerLock.acquire()
        choice = random.randint(0,len(workers)-1) #can choose a random number based on keys as well 
        if workers[choice]['freeSlots']>0:
            workers[choice]['freeSlots']-=1
            mouth1=socket.socket()
            mouth1.connect(('Localhost',workers[choice]['port']))
            msg=json.dumps(task).encode()
            mouth1.send(msg)
        workerLock.release()
        sleep(0.001)

def RR(task,workers,algo):
    i=0
    while True:
        workerLock.acquire()
        if workers[i]['freeSlots']>0:
            workers[i]['freeSlots']-=1
            mouth2=socket.socket()
            mouth2.connect(('Localhost',workers[i]['port']))
            msg=json.dumps(task).encode()
            mouth2.send(msg)
            workerLock.release()
            break
        workerLock.release()
        i=(i+1)%len(workers)
        if i==0:
            sleep(1)

def LL(task,workers,algo):
    while True:
        workerLock.acquire()
        min_i=0
        min_free=workers[0]['freeSlots']
        for i in range(1,len(workers)):
            if workers[i]['freeSlots']>min_free:
                min_free=workers[i]['freeSlots']
                min_i=i
        if min_free:
            workers[min_i]['freeSlots']-=1
            mouth3=socket.socket()
            mouth3.connect(('Localhost',workers[min_i]['port']))
            msg=json.dumps(task).encode()
            mouth3.send(msg)
            workerLock.release()
            break
        workerLock.release()
        sleep(1)

def yeetacs(jobs, algo, workers):
    algorithm={'RANDOM':RANDOM, 'RR':RR,'LL':LL}
    while True:
        jobLock.acquire()
        for job in jobs:
            if len(jobs[job]['mapTasks'])>0:
                for i in range(len(jobs[job]['mapTasks'])):
                    if not jobs[job]['mapTasks'][i]['scheduled']:
                        task={'jobID':jobs[job]['jobID'],'taskID':jobs[job]['mapTasks'][i]['task_id'],'time':jobs[job]['mapTasks'][i]['duration'],'algo':algo}
                        algorithm[algo](task,workers,algo)
                        jobs[job]['mapTasks'][i]['scheduled']=True
            else:
                for i in range(len(jobs[job]['reduceTasks'])):
                    if not jobs[job]['reduceTasks'][i]['scheduled']:
                        task={'jobID':jobs[job]['jobID'],'taskID':jobs[job]['reduceTasks'][i]['task_id'],'time':jobs[job]['reduceTasks'][i]['duration'],'algo':algo}
                        algorithm[algo](task,workers,algo)
                        jobs[job]['reduceTasks'][i]['scheduled']=True
        jobLock.release()
        sleep(0.001)

def getJobRequests(jobs):
    ear = socket.socket() #Establishing a TCP connection for job socket 
    ear.bind(('',5000))
    ear.listen(500) #Can change this param if needed - to listen 
    while(True):
        connection,address = ear.accept()
        requests = connection.recv(65536).decode()
        arrival_time=datetime.datetime.now()
        job = json.loads(requests)
        jobLock.acquire()
        workerLock.acquire()
        log=open('masterlog.txt','a')
        log.write(str(arrival_time)+',JobArrived,'+job['job_id']+'\n')
        log.close()
        workerLock.release()
        print(job)
        jobID = job['job_id']
        jobs[jobID] = {'mapTasks':job["map_tasks"], "reduceTasks":job["reduce_tasks"], 'jobID':jobID}
        for map_task in jobs[jobID]['mapTasks']:
            map_task['scheduled']=False
        for reduce_task in jobs[jobID]['reduceTasks']:
            reduce_task['scheduled']=False
        print(jobs)
        jobLock.release()
        connection.close()
        sleep(0.001)

def getWorkerMessage(workers):
    ear2=socket.socket()
    ear2.bind(('',5001))
    ear2.listen(256)
    while True:
        connection,address=ear2.accept()
        msg=connection.recv(1024).decode()
        removedTask=json.loads(msg)
        workerLock.acquire()
        workers[int(removedTask['workerID'])-1]['freeSlots']+=1
        workerLock.release()
        jobLock.acquire()
        if 'M' in removedTask['taskID']:
            jobs[removedTask['jobID']]['mapTasks'].pop([int(removedTask['taskID'].split('M')[-1])])
        else:
            jobs[removedTask['jobID']]['reduceTasks'].pop([int(removedTask['taskID'].split('R')[-1])])
            if len(jobs[removedTask['jobID']]['reduceTasks'])==0:
                log=open('masterlog.txt','a')
                log.write(str(datetime.datetime.now())+',JobFinished,'+jobs['job_id']+'\n')
                log.close()
                jobs.pop(removedTask['jobID'])
        jobLock.release()
        connection.close()
        sleep(0.001)

conf_file=sys.argv[1]
algo=sys.argv[2]
config = open(conf_file)
config=json.load(config)
workers = config['workers']
for worker in workers:
    worker['freeSlots'] = worker['slots']
log=open('masterlog.txt','w')
log.close()
jobs={}
thread1 = threading.Thread(target = getJobRequests, args=(jobs,))
thread2 = threading.Thread(target = getWorkerMessage, args=(workers,))
thread3 = threading.Thread(target = yeetacs, args=(jobs, algo, workers))
thread1.start()
thread2.start()
thread3.start()
thread1.join()
thread2.join()
thread3.join()