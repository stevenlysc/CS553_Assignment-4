# coding: utf-8
import argparse
import socket

class Client(object):
    def __init__(self, ip, port, workload_file):
        self.ip = ip
        self.port = int(port)
        self.workload_file = workload_file
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
        print 'Sending tasks...\n'
        tasks = self.get_Tasks()

        client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        client_socket.connect((self.ip, self.port))

        for taskId in range(len(tasks)):
            msg = 'Task {}: {}' .format(taskId, tasks[taskId])
            client_socket.send('{}\n' .format(msg))
            print '{} is sent to scheduler successfully' .format(msg)
        
        client_socket.send('Q')
        print '\nAll tasks have been sent to scheduler successfully.\n'

        client_socket.close()
        return
    
    def receive_Result(self):
        print 'Waiting for the results...\n'

        client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        client_socket.bind(('', self.port + 100))
        client_socket.listen(5)

        conn, addr = client_socket.accept()
        result = str()
        while 1:  
            char = conn.recv(1)
            if char == 'Q':
                break
            elif char == '\n':
                print result
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
    # client -s <IP_ADDRESS:PORT> -w <WORKLOAD_FILE>
    parser = argparse.ArgumentParser()
    parser.add_argument('-s', metavar='IP_ADDRESS:PROT', type=str, required=True, 
                        help='the location of the front-end scheduler.')
    parser.add_argument('-w', metavar='WORKLOAD_FILE', type=str, required=True, 
                        help='the local file that will store the tasks that need to be submitted to the scheduler.')

    args = parser.parse_args()
    #if len(args) != 2:
    #    print 'usage: client -s <IP_ADDRESS:PORT> -w <WORKLOAD_FILE>'
    #    exit(-1)
    
    ip = args.s.split(':')[0]
    port = args.s.split(':')[1]
    workload_file = args.w
    
    client = Client(ip, port, workload_file)
    
    client.send_Tasks()
    client.receive_Result()

