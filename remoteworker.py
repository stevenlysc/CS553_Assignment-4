
import sys
import getopt
import boto.sqs
from boto.sqs.message import Message
import datetime
import boto.ec2
import socket
import time
import boto.dynamodb
def getArgs():
    global threadholdTime
    shortargs="i:"
    longargs=["time="]
    options,values=getopt.getopt(sys.argv[1:], shortargs, longargs)  
    for option,value in options:
        if option=="-i":
            threadholdTime=value      
                    
if __name__=="__main__":
    
    threadholdTime=-1
    getArgs()
    if threadholdTime<0:
        print "Wrong parameter!!!"
        sys.exit(0)
    connSQS=boto.sqs.connect_to_region(
            "us-east-2", 
            aws_access_key_id='AKIAJHOSSATSRBTKMY5Q',
            aws_secret_access_key='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')
    connEC2=boto.ec2.connect_to_region(
            "us-east-2", 
            aws_access_key_id='AKIAJHOSSATSRBTKMY5Q',
            aws_secret_access_key='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')
    connDDB=boto.dynamodb.connect_to_region(
            "us-east-2", 
            aws_access_key_id='AKIAJHOSSATSRBTKMY5Q',
            aws_secret_access_key='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')
    taskQueue = connSQS.get_queue('taskQueue')
    resultQueue = connSQS.get_queue('resultQueue')
    table = connDDB.get_table('taskDB')
    m=Message()    
    duration=0
    tag=0
    m=Message()
    m.set_body("123")
    taskQueue.write(m)
    while True:      
        rs=taskQueue.get_messages()
        if tag==0:
            starttime=datetime.datetime.now()
        if len(rs)==0:                  
            tag=1
            endtime=datetime.datetime.now()
            duration=(endtime-starttime).seconds
            if duration>int(threadholdTime) and int(threadholdTime)!=0:       
                reservations=connEC2.get_all_reservations()
                instances=reservations[0].instances
                localIP=socket.gethostbyname(socket.gethostname())
                for instance in instances:
                    if instance.private_ip_address==localIP:
                        connEC2.terminate_instances(instance_ids=[instance.id])
                break
        else: 
            tag=0
            m=rs[0]
            body=m.get_body()
            taskID=str(body).split(":")[0]
            taskcontent=str(body).split(":")[1]
            item=table.get_item(hash_key=taskID)
            if item!=None:
                print "Other works are executing %s" % taskID
            elif item==None:
                try:
                    millisecond=taskcontent.split(" ")[1]
                    second=float(millisecond)/100.0
                    item_data={'taskContent':taskcontent}
                    item=table.new_item(hash_key=taskID, attrs=item_data)
                    item.put()
                    time.sleep(second)
                    item=table.get_item(hash_key=taskID)
                    item.delete()
                    result=body+"---0"
                    taskQueue.delete_message(m)
                except ValueError:
                    result=body+"---1"
                finally:
                    print "Completed-> %s" % result
                    m.set_body(result)
                    resultQueue.write(m)   
