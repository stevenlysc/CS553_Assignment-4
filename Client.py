
# coding: utf-8

# In[5]:

import argparse
import socket


class Client(object):
    def __init__(self, ip, port, workload_file, client_socket):
        self.ip = ip
        self.port = port
        self.workload_file = workload_file
        self.client_socket = client_socket
        return
    
    def get_Tasks(self):
        tasks = list()
        try:
            fstream = open(self.workload_file, 'r')
            fline = fstream.readline()
            while fline:
                tasks.append(fline.replace('\n', ''))
                fline = fstream.readline()
            fstream.close()
        except IOError, e:
            print e
        return tasks
    
    def send_Tasks(self):
        tasks = self.get_Tasks()
        
        for taskId in range(len(tasks)):
            msg = 'Task {}: {}' .format(taskId, tasks[taskId])
            self.client_socket.send(msg)
            print '{} is sent to scheduler successfully' .format(msg)
        
        print 'All tasks have been sent to scheduler successfully.'
        return
    
    def receive_Result(self):
        print 'Waiting for the results...'
        while True:  
            result = self.client_socket.recv(1024)
            if result:
                print result
            else:
                pass
        return
    
    # Test-Oriented Methods
    def show_Args(self):
        print 'client -s {}:{} -w {}' .format(self.ip, self.port, self.workload_file)
        return
    
    def show_Tasks(self):
        tasks = self.get_Tasks()
        print tasks
        return

if __name__ == '__main__':
    
    # client -s <IP_ADDRESS:PORT> -w <WORKLOAD_FILE>
    parser = argparse.ArgumentParser()
    parser.add_argument('-s', metavar='IP_ADDRESS:PROT', type=str, required=True, 
                        help='the location of the front-end scheduler.')
    parser.add_argument('-w', metavar='WORKLOAD_FILE', type=str, required=True, 
                        help='the local file that will store the tasks that need to be submitted to the scheduler.')

    args = parser.parse_args()
    if len(args) != 2:
        print 'usage: client -s <IP_ADDRESS:PORT> -w <WORKLOAD_FILE>'
        exit(-1)
    
    ip = args.s.split(':')[0]
    port = args.s.split(':')[1]
    workload_file = args.w
    client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    
    client = Client(ip, port, workload_file, client_socket)
    
    client.show_Args()
    client.show_Tasks()
    
    client.send_Tasks()
    client.receive_Result()

