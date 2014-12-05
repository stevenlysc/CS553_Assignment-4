import sys
import getopt
import socket
import threadpool
import time
import thread
import boto.sqs
from boto.sqs.message import Message
  
def getArgs():
    global port,num,tag
    shortargs="s:"
    longargs=["port=","lw=","rw"]
    options,values=getopt.getopt(sys.argv[1:], shortargs, longargs)  
    for option,value in options:
        if option=="-s":
            port=value      
        if option=="--lw":
            num=value
        if option=="--rw":
            pass
        if num!=0 and tag==1:
            print "Wrong parameter!!!"
            sys.exit(0)

def recvTask():  
    global schedulerSock,task,lists,test,connection
    while True:
        print("I am waiting task from client...")
        connection,address=schedulerSock.accept()
        while True:
            data=connection.recv(1024)
            if data=="End of task": 
                break
            data=data.strip()
            test.append(data) 
        print("I receive %d tasks from client!"%len(test))
        break
         
def createWorkers(threadID):
    global num,test
    if num!=0 and len(test)!=0:    
        pool=threadpool.ThreadPool(int(num))
        requests=threadpool.makeRequests(doTask,test,print_result)
        [pool.putRequest(req) for req in requests]
        pool.wait()
    else:
        pass

def doTask(unexecutedTask): 
    global test
    try:
        millisecond=unexecutedTask.split(":")[1].split(" ")[1]
        second=float(millisecond)/100.0
        time.sleep(second)
        test.remove(unexecutedTask)   
        result=unexecutedTask+"---0"     
    except ValueError:
        result=unexecutedTask+"---1"
    finally:
        return result

def print_result(request,result):
    global taskResult
    print "Completed-> %s" % result
    taskResult.append(result)

def sendResults(threadID):
    global taskResult,connection
    while True:
        if len(taskResult)>0:
            for item in taskResult:
                print "I am sending results %s" % item
                connection.send(item)
                taskResult.remove(item)
                      
if __name__=="__main__":    
    ipaddress=""
    port=""
    num=0
    test=[]
    getArgs()
    schedulerSock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    schedulerSock.bind((ipaddress, int(port)))
    schedulerSock.listen(1)
    recvTask()
    if num!=0:
        taskResult=[] 
        createWorkers()
        sendResult()    		
    elif num==0:
        print "I am sending tasks to sqs"
        conn=boto.sqs.connect_to_region("us-east-1")
        taskQueue=conn.create_queue('taskQueue')
        resultQueue=conn.create_queue('resultQueue')
        m=Message()
        for item in test:
            m.set_body(item)
            taskQueue.write(m)
        print "All tasks are sent to sqs"
        while True:
            if resultQueue.count()>0:                
                rs=resultQueue.get_messages()
                if len(rs)>0:
                    m=rs[0]
                    body=m.get_body()
                    print "I am sending results %s" % body
                    connection.send(body)
                    resultQueue.delete_message(m)
                else:
                    pass
    
