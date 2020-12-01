import json
import socket
import sys
from time import sleep
import random
import threading
import datetime
def taskStart(details, newTask):
    lockB.acquire()
    log=open('log.txt','a')
    log.write(details['workerID']+str(datetime.datetime.now())+'TaskStarting'+newTask['jobID']+newTask['taskID']+newTask['time'], sep=',')
    log.close()
    lockB.release()
    i=0
    while i<details['numSlots'] and details['tasks'][i][1]==True:
        i+=1
    details['tasks'][i][0]=newTask
    details['tasks'][i][1]=True
    details['freeSlots']-=1
def listenNewTasks(details):
    ear=socket.socket()
    ear.bind((details['sender'],int(details['portNumber'])))
    ear.listen()
    while(True):
        connection, address=ear.accept()
        msg=connection.recv(1024).decode()
        if msg:
            msg=json.loads(msg)
            newTask={'jobID':msg['jobID'],'taskID':msg['taskID'],'timeLeft':msg['time']}
            lockB.acquire()
            log=open('log.txt','a')
            log.write(details['workerID']+str(datetime.datetime.now())+'TaskArrived'+msg['jobID']+msg['taskID']+msg['time'], sep=',')
            log.close()
            lockB.release()
            lockA.acquire()
            taskStart(details,newTask)
            lockA.release()
def execution(details):
    while details['numSlots']==0:
        continue
    i=0
    while True:
        if details['tasks'][i][1] and details['tasks'][0]['timeLeft']>0:
            lockA.acquire()
            details['tasks'][i][0]['timeLeft']-=1
            lockA.release()
        elif details['tasks'][i][1] and details['tasks'][0]['timeLeft']<=0:
            removeTask=details['tasks'][i][0]
            lockB.acquire()
            log=open('log.txt','a')
            log.write(details['workerID']+str(datetime.datetime.now())+'TaskFinished'+removeTask['jobID']+removeTask['taskID']+removeTask['time'], sep=',')
            log.close()
            lockB.release()
            lockA.acquire()
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
lockA=threading.Lock()
lockB=threading.Lock()
details={'workerID':sys.argv[2], 'portNumber':sys.argv[1], 'numSlots':0}
conf_file=open('config.json')
config=json.load(conf_file)
for worker in config['workers']:
    if worker['worker_id']==details['portNumber']:
        details['numSlots']=worker['slots']
        details['tasks']=[({},False) for i in range(worker['slots'])]
        details['freeSlots']=details['numSlots']
conf_file.close()
log=open("log.txt",'w') #to empty the contents
log.close()
details['sender']=socket.gethostbyname('localhost')
thr1=threading.Thread(target=listenNewTasks, args=(details,))
thr2=threading.Thread(target=execution, args=(details,))
thr1.start()
thr2.start()
thr1.join()
thr2.join()