#Making the necessary imports 
import sys 
import os 
import operator 
import random
import socket
from thread import *
import threading 
import csv
import math 
import seaborn  
from datetime import sleep,time #should this be just time module ?

#path to config - 
config_path = sys.argv[1]
# if it is needed to take cli arguments 
# sched_algo = sys.argv[2]

#Uncomment the scheduling algorithm to see its implementation 

#Implementation of the scheduling algorithms
 
#Creating datastructures  
worker_state = dict()
job_state = dict()
map_tasks = list()
reduce_tasks = list()

#Assigning a lock for each 
worker_state_lock = threading.Lock()
job_state_lock = threading.Lock()
map_tasks_lock = threading.Lock()
reduce_tasks_lock = threading.Lock()


#1. Random Scheduling 
def random_scheduling():
    while True:
        workers_state_lock.acquire()
        choice = random.randint(1,3) #can choose a random number based on keys as well 
        free_slots = worker_state[choice]['slots'] - worker_state[choice]['occupied slots']
        #can include a print statement here to check for choice 
        if free_slots > 0:
            print(choice)
            #if slots in worker are free - increment occupied and release lock 
            worker_state[choice]['occupied_slots'] += 1
            worker_state_lock.release()

        worker_state_lock.release()
        sleep(0.005)


'''  
#2. Round Robin 
def round_robin():

#3. Least-Loaded 
def least_loaded():
'''

#the actual scheduler 
def yeetacs():

    print("ahahaha") #debug 

    #to decide on sched algo - can be taken from the command line 
    if sched_algo == "Random":
        worker_id = random_scheduling()
    elif sched_algo == "RR":
        worker_id = round_robin()
    elif sched_algo == "LL":
        worker_id == least_loaded()
    else:
        print("Enter correct arguments else Big rip for worker")

    #need to acquire and release locks and also connect socket to localhost 
    


    
#Assigning the task to the scheduler - 




#Listen to job requests
def getJobRequests():
    host = ""
    port = 5000 #listens on port 5000 for jobs and 5001 for workers 
    job_socket = socket.socket(socket.AF_INET,socket.SOCK_STREAM) #Establishing a TCP connection for job socket 
    job_socket.bind(host,port)
    job_socket.listen(500) #Can change this param if needed - to listen 

    while(True):
        connection,address = job_socket.accept()
        received = connection.rcv(2048)
        #print arrival time using datetime.now()
        job = json.load(received)

        #acquire locks 
        map_tasks_lock.acquire()
        job_state_lock.acquire()

        job_id = job['job_id']
        job_state[job_id] = dict()
        job_state[job_id]["arrival_time"] = arrival_time
        job_state[job_id]["map tasks"] = job_state["map tasks"]
        job_state[job_id]["map completed"] = False
        job_state[job_id]["total completed"] = False 

        for i in range(len(job["map_tasks"])):
            job_state[job_id]["map_tasks"][i]['job_id'] = job_id
        
        job_state[job_id]["reduce_tasks"] = job["reduce_tasks"]

        for i in range(len(job["reduce_tasks"])):
            job_state[job_id]["reduce_tasks"][i]['job_id'] = job_id

        job_state[job_id]["map_tasks_completed"] = list()
        job_state[job_id]["reduce_tasks_completed"] = list()

        map_tasks.extend(job_state[job_id]["map_tasks"])

        #release locks 
        job_state_lock.release()
        map_tasks_lock.release()

        connection.close()


#Listen to worker requests  
def getWorkerRequests():











#Main function 

def main():
    #make a separate directory to log file 
    try:
        os.mkdir(log)
    except:
        pass #log directory already exists from a previous test run 

    #Open the log file on master to write on it 
    f = open('log/master.txt','w')
    f.close() #close the file after writing logs 
    config = open('config_path')
    json.load(config)
    #print(config) - uncomment if needed

    #creating a global variable for worker slots and filling it using dict as the implemented data stucture
    global workerSlots
    
    #creating a json object for workers
    workers = config['workers']
    for worker in workers:
        worker_state = worker['worker_id']
    
    for state in worker_state:
        worker_state[state]['occupied_slots'] = 0
    
    #A lock to be released here for worker?

    #debugging - check if number of slots are proper
    print(workerSlots)

    #getting the actual worker threads and assigning them targets to run the jobs on 
    thread1 = threading.thread(target = getJobRequests)
    thread2 = threading.thread(target = getWorkerRequests)
    thread3 = threading.thread(target = yeetacs)

    #Starting the worker threads for communication with the master
    thread1.start()
    thread2.start()
    thread3.start()

    #Joining the above memtioned thread 
    thread1.join()
    thread2.join()
    thread3.join()

    #Pls work
    print("It works!")

#Running the main function 
if __name__ == '__main__':
    main()