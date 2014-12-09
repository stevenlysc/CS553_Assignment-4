# coding: utf-8
import argparse
import socket
import time

class Client(object):

    # Initializing...
    def __init__(self, ip, port, workload_file):
        self.ip = ip
        self.port = int(port)
        self.workload_file = workload_file
        return
    
    #Get tasks from job file
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
    
    #Using socket, send all the tasks to the scheduler
    def send_Tasks(self):
        print 'Sending tasks...'
        tasks = self.get_Tasks()

        client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        client_socket.connect((self.ip, self.port))

        for taskId in range(len(tasks)):
            msg = 'Task {}: {}' .format(taskId, tasks[taskId])
            client_socket.send('{}\n' .format(msg))
            print '\t{} is sent to scheduler successfully' .format(msg)
        
        client_socket.send('Q')
        print 'All tasks have been sent to scheduler successfully.\n'

        client_socket.close()
        return
    
    #Using socket, listen the socket and receive the results from the scheduler. 
    def receive_Result(self):
        print 'Waiting for the results...'

        client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        client_socket.bind(('', self.port + 100))
        client_socket.listen(5)

        conn, addr = client_socket.accept()
        result = str()
        while 1:  
            char = conn.recv(1)
            if char == '`':
                break
            elif char == '\n':
                print '\t' + result
                result = str()
            else:
                result = result + str(char)
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

    #Parse argument
    #Format: client -s <IP_ADDRESS:PORT> -w <WORKLOAD_FILE>
    parser = argparse.ArgumentParser()
    parser.add_argument('-s', metavar='IP_ADDRESS:PROT', type=str, required=True, 
                        help='the location of the front-end scheduler.')
    parser.add_argument('-w', metavar='WORKLOAD_FILE', type=str, required=True, 
                        help='the local file that will store the tasks that need to be submitted to the scheduler.')

    args = parser.parse_args()
    #if len(args) != 2:
    #    print 'usage: client -s <IP_ADDRESS:PORT> -w <WORKLOAD_FILE>'
    #    exit(-1)
    
    #obtain the ip address, port and job file location from argument
    ip = args.s.split(':')[0]
    port = args.s.split(':')[1]
    workload_file = args.w
    
    client = Client(ip, port, workload_file)
    
    #Set timer, start doing the job
    start = time.time()
    client.send_Tasks()
    client.receive_Result()
    end = time.time()
    print 'Elapsed: {} ms' .format(1000 * (end - start))

