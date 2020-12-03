import json
import socket
import sys
from time import sleep
import random
import threading
import datetime
#function to assign newTask to a slot and start executing it (called by listenNewTasks function)
def taskStart(details, newTask):
    lockB.acquire()
    log=open('workerlog.txt','a')
    log.write(str(details['workerID'])+','+str(datetime.datetime.now())+',TaskStarting'+','+str(newTask['jobID'])+','+newTask['taskID']+','+str(newTask['timeLeft'])+','+newTask['algo']+'\n')
    log.close()
    lockB.release()
    i=0
    #Search for an empty slot
    while i<details['numSlots'] and details['tasks'][i][1]==True:
        i+=1
    details['tasks'][i][0]=newTask
    details['tasks'][i][1]=True
    details['freeSlots']-=1
#function to listen to new tasks from the master (runs separately as a thread)
def listenNewTasks(details):
    while details['numSlots']==0:
        continue
    ear=socket.socket()
    ear.bind(('',int(details['portNumber'])))
    ear.listen(256)
    while(True):
        connection=ear.accept()
        msg=connection[0].recv(1024).decode()
        #read if message received
        if msg:
            msg=json.loads(msg)
            newTask={'jobID':msg['jobID'],'taskID':msg['taskID'],'timeLeft':msg['time'],'algo':msg['algo']}
            lockB.acquire()
            log=open('workerlog.txt','a')
            log.write(str(details['workerID'])+','+str(datetime.datetime.now())+',TaskArrived'+','+str(msg['jobID'])+','+msg['taskID']+','+str(msg['time'])+','+msg['algo']+'\n')
            log.close()
            lockB.release()
            lockA.acquire()
            taskStart(details,newTask)
            lockA.release()
        connection[0].close()
        sleep(0.001)
#function to execute tasks (runs separately as a thread)
def execution(details):
    while details['numSlots']==0:
        continue
    i=0
    while True:
        #if task not yet over
        if details['tasks'][i][1] and details['tasks'][i][0]['timeLeft']>0:
            lockA.acquire()
            #decrement time for each task
            details['tasks'][i][0]['timeLeft']-=1
            lockA.release()
        #if task over, remove it by sending update to master
        elif details['tasks'][i][1] and details['tasks'][i][0]['timeLeft']<=0:
            removeTask=details['tasks'][i][0]
            removeTask['workerID']=details['workerID']
            lockB.acquire()
            log=open('workerlog.txt','a')
            log.write(str(details['workerID'])+','+str(datetime.datetime.now())+',TaskFinished'+','+str(removeTask['jobID'])+','+removeTask['taskID']+','+str(removeTask['timeLeft'])+','+removeTask['algo']+'\n')
            log.close()
            lockB.release()
            lockA.acquire()
            #make the slot available for future use
            details['tasks'][i][1]=False
            details['freeSlots']+=1
            lockA.release()
            mouth=socket.socket()
            mouth.connect(('localhost',5001))
            msg=json.dumps(removeTask).encode()
            mouth.send(msg)
            mouth.close()
        i=(i+1)%details['numSlots']
        if i==0:
            sleep(1)
        sleep(0.001)
#Lock that controls updation of details dictionary
lockA=threading.Lock()
#Lock that controls log writes
lockB=threading.Lock()
details={'workerID':sys.argv[2], 'portNumber':sys.argv[1], 'numSlots':0}
with open('config.json') as conf_file:
    config=json.load(conf_file)
    for worker in config['workers']:
        #search for worker with the same worker_id
        if int(worker['worker_id'])==int(details['workerID']):
            details['numSlots']=worker['slots']
            #add key-value pair called tasks that has value as a list of n lists with task info dictionary as bool to check if task is allocated where n is the number of slots
            details['tasks']=[[{},False] for i in range(worker['slots'])]
            details['freeSlots']=details['numSlots']
            break
details['sender']=socket.gethostbyname('localhost')
thr1=threading.Thread(target=listenNewTasks, args=(details,))
thr2=threading.Thread(target=execution, args=(details,))
thr1.start()
thr2.start()
thr1.join()
thr2.join()