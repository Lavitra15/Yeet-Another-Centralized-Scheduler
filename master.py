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
            mouth=socket.socket()
            mouth.connect(('Localhost'),workers[choice]['port'])
            msg=json.dumps(task).encode()
            mouth.send(msg)
        workerLock.release()

def RR(task,workers,algo):
    i=0
    workerLock.acquire()
    while True:
        if workers[i]['freeSlots']>0:
            workers[i]['freeSlots']-=1
            mouth=socket.socket()
            mouth.connect(('Localhost'),workers[i]['port'])
            msg=json.dumps(task).encode()
            mouth.send(msg)
            workerLock.release()
            break
        i=(i+1)%len(workers)
        if i==0:
            sleep(1)

def LL(task,workers,algo):
    workerLock.acquire()
    while True:
        min_i=0
        min_free=workers[0]['freeSlots']
        for i in range(1,len(workers)):
            if workers[i]['freeSlots']>min_free:
                min_free=workers[i]['freeSlots']
                min_i=i
        if min_free:
            workers[min_i]['freeSlots']-=1
            mouth=socket.socket()
            mouth.connect(('Localhost'),workers[min_i]['port'])
            msg=json.dumps(task).encode()
            mouth.send(msg)
            workerLock.release()
            break
        sleep(1)

def yeetacs(jobs, algo, workers):
    algorithm={'RANDOM':RANDOM, 'RR':RR,'LL':LL}
    while True:
        for job in jobs:
            if len(jobs[job]['mapTasks'])>0:
                task={'jobID':job,'taskID':jobs[job]['mapTasks'][0]['task_id'],'time':jobs[job]['mapTasks'][0]['duration']}
                algorithm[algo](task,workers,algo)
            else:
                task={'jobID':job,'taskID':jobs[job]['reduceTasks'][0]['task_id'],'time':jobs[job]['reduceTasks'][0]['duration']}
                algorithm[algo](task,workers,algo)

def getJobRequests(jobs):
    ear = socket.socket() #Establishing a TCP connection for job socket 
    ear.bind(('',5000))
    ear.listen(500) #Can change this param if needed - to listen 
    while(True):
        connection,address = ear.accept()
        requests = connection.recv(65536).decode()
        arrival_time=datetime.datetime.now()
        job = json.load(requests)
        jobLock.acquire()
        workerLock.acquire()
        log=open('masterlog.txt','a')
        log.write(arrival_time+'JobArrived'+job['job_id'])
        log.close()
        workerLock.release()
        jobID = job['job_id']
        jobs[jobID] = {'mapTasks':job["map tasks"], "reduceTasks":job["reduce_tasks"]}
        jobLock.release()
        connection.close()

def getWorkerMessage(workers):
    ear2=socket.socket()
    ear2.bind(('',5001))
    ear2.listen(len(workers))
    while True:
        connection,address=ear2.accept()
        msg=connection.recv().decode()
        removedTask=json.loads(msg)
        workerLock.acquire()
        workers[removedTask['workerID']]['freeSlots']+=1
        workerLock.release()
        jobLock.acquire()
        if 'M' in removedTask['taskID']:
            jobs[removedTask['workerID']]['mapTasks'].pop([int(removedTask['taskID'].split('M')[-1])])
        else:
            jobs[removedTask['workerID']]['reduceTasks'].pop([int(removedTask['taskID'].split('R')[-1])])
            if len(jobs[removedTask['workerID']]['reduceTasks'])==0:
                workerLock.acquire()
                log=open('masterlog.txt','a')
                log.write('JobFinished'+jobs['job_id'])
                log.close()
                workerLock.release()
                jobs.pop(removedTask['jobID'])
        jobLock.release()
        connection.close()

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