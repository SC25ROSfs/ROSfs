from __future__ import print_function

import sys
import os
import argparse
import multiprocessing as mp
import time
import json
import subprocess
import re
import collections

import rosbag
import rospy
import zmq
import cProfile

from natsort import natsorted

pr = cProfile.Profile()

POLICY = ['latest', "subsample", "compression"]

with open("./topic-abbr.json", 'r') as f:
    bag2topic = json.load(f)

MB2B = 1048576
INT_MAX = 2147483647

"""
Code from https://www.oreilly.com/library/view/python-cookbook/0596001673/ch05s19.html
"""
class RingBuffer:
    """ class that implements a not-yet-full buffer """
    def __init__(self, size_max):
        self.max = size_max
        self.data = []

    class __Full:
        """ class that implements a full buffer """
        def append(self, x):
            """ Append an element overwriting the oldest one. """
            self.data[self.cur] = x
            self.cur = (self.cur+1) % self.max
        def get(self):
            """ return list of elements in correct order """
            return self.data[self.cur:]+self.data[:self.cur]

    def append(self,x):
        """append an element at the end of the buffer"""
        self.data.append(x)
        if len(self.data) == self.max:
            self.cur = 0
            # Permanently change self's class from non-full to full
            self.__class__ = self.__Full

    def get(self):
        """ Return a list of elements from the oldest to the newest. """
        return self.data

class WifiMonitor:
    def __init__(self, buffer_size, wlp_name=None, simulate=False):
        self.buffer = RingBuffer(buffer_size)
        self.wifi_on = True
        self.wlp_name = wlp_name
        self.sim = simulate
        self.signal_levels = mp.Manager().list()

    def monitor_wifi(self):
        pattern = r"Signal level=(-?\d+) dBm"
        last_time = time.time()
        count = 0
        if self.sim:
            with open(self.sim, 'r') as f:
                self.dbm_cache = list(json.load(f, object_pairs_hook=collections.OrderedDict).values())
        while True:
            if time.time() - last_time >= 0.5:
                if self.wlp_name:
                    proc = subprocess.Popen(['iwconfig', self.wlp_name], stdout=subprocess.PIPE)
                    stdout = proc.communicate()[0].decode()
                    match = re.search(pattern, stdout)
                    cur_signal = match.group(1)
                    # print(cur_signal)
                    self.signal_levels.append(int(cur_signal))
                else:
                    cur_signal = self.dbm_cache[count]
                    print(cur_signal)
                    self.signal_levels.append(cur_signal)
                    count += 1
                
                last_time = time.time()

    def start_monitoring(self):
        self.wifi_process = mp.Process(target=self.monitor_wifi)
        self.wifi_process.daemon = True
        self.wifi_process.start()

    def kill_monitoring(self):
        self.wifi_on = False
        self.wifi_process.join()
    

class ContainerServer:
    def __init__(self, host, port, wifi_on, path, duration=5, bag_format='rosfs'):
        self.socket = zmq.Context().socket(zmq.REP)
        # self.addr = f"tcp://{host}:{port}"
        self.addr = "tcp://"+ host + ":" + port
        self.socket.bind(self.addr)
        self.policy = POLICY[0]
        self.backend = path
        self.query_start = False
        
        self.bag_name = None
        self.msg_queue = {}         # {id, [(msg_data, timestamp)...]}
        self.bandwidth = 0          # random bandwidth range from (1MB-5MB/s)
        self.wifi_on = wifi_on
        self.bag_format = bag_format # ['rosfs', 'ros1']
        self.duration = duration
        
        print("Current pyzmq version is {}".format(zmq.__version__))

    def get_wifi_monitor(self, monitor):
        self.wifi_monitor = monitor

    def listening(self):
        while True:
            #  Wait for next request from client
            # self.bandwidth = random.randint(1024**2 * 2, 1024**2 * 5)
            message = self.socket.recv().decode()
            if message == b'':
                self.socket.send(b"")
                continue
            tokens = message.split()
            
            if tokens[0] == "kill":
                self.socket.send(b"")
                self.socket.close()
                if self.wifi_on:
                    with open("wifi_signal.json", 'w') as f:
                        json.dump(list(self.wifi_monitor.signal_levels), f, indent=2)
                break

            elif tokens[0] == "BAG":
                if len(tokens) > 1 and self.backend != None:
                    self.bag_name = self.backend + '/' +  tokens[1]
                    if os.path.exists(self.bag_name) == False:
                        print("BAG does not exist")
                        self.bag_name = None
                        self.socket.send(b'')
                        continue
                    print("START BAG {} QUERY".format(tokens[1]))
                    self.socket.send(b'')
                else:
                    self.socket.send(b'')

            elif self.bag_name is None:
                print(message)
                self.socket.send(b"")

            elif tokens[0] == 'q':
                print("Query topics: {}".format(tokens[1:len(tokens)-1]))
                topics = [bag2topic[t] for t in tokens[1:-1]]
                time_len = float(tokens[-1])
                if self.bag_format == 'ros1':
                    self.read_ros1_latest_msg(topics, time_len)
                else:
                    self.read_latest_msg(topics, time_len)
            
            elif tokens[0] == 'qt':
                start, end = float(tokens[-2]), float(tokens[-1])
                topics = [bag2topic[t] for t in tokens[1:-2]]
                if self.bag_format == 'ros1':
                    self.read_ros1_ranged_msg(topics, start, end)
                else:
                    self.read_ranged_msg(topics, start, end)
            
            elif tokens[0] == 'qa':
                topics = [bag2topic[t] for t in tokens[1:]]
                self.read_auto_msg(topics, 5)
            
            else:
                self.socket.send_multipart([b"ERROR", b"query format: q topic1 topic2..  time_len"])
    
    # a quick read to get the msg size of target topic
    def get_msg_size(self, bag):
        topic_sizes = {}
        topic_conn = bag.read_rosfs_topic_header()
        all_topics = list(topic_conn.keys())
        for topic in all_topics:
            for _, m, _ in bag.read_messages([topic], raw=True):
                if topic in self.topic_sizes:
                    break
                self.topic_sizes[topic] = len(m[1])
        return topic_sizes

    def read_latest_msg(self, topics, time_len):
        start = time.time()
        pr.enable()
        bag = rosbag.Bag(self.bag_name, 'rosfs')
        end_time = bag.get_end_time() - 1 # TODO: eliminate read error
        start_time = rospy.Time(end_time - time_len)
        end_time = rospy.Time(end_time)
        print(topics, time_len)
        for topic, m, t in bag.read_messages(topics, start_time, end_time, raw=True):
            id, data = m[0], m[1]
            if id not in self.msg_queue.keys():
                self.msg_queue[id] = []
            self.msg_queue[id].append(data)
        pr.disable()
        end = time.time()
        print("read msg took %.4f seconds" % (end-start))

        con_info, msg_count = '', 0
        byte_data = bytearray()
        for i in self.msg_queue.keys():
            con_info += str(i) + ' '
            msgs = self.msg_queue[i]
            for msg in msgs:
                msg_count += 1
                byte_data.extend(msg)
        con_info += '/' + str(end-start) + '/' + str(msg_count)
        print("send {} {} messages from last {} seconds, size: {:.4f} KB".format(msg_count, topics, time_len, len(byte_data) / 1024))
        
        self.socket.send_multipart([con_info.encode(), byte_data])
        self.msg_queue.clear()

    def read_ranged_msg(self, topics, start_time, end_time):
        start = time.time()
        pr.enable()
        bag = rosbag.Bag(self.bag_name, 'rosfs')
        bag_start_time = int(bag.get_start_time())
        start_time = bag_start_time + start_time
        end_time = bag_start_time + end_time
        for topic, m, t in bag.read_messages(topics, start_time, end_time, raw=True):
            id, data = m[0], m[1]
            if id not in self.msg_queue.keys():
                self.msg_queue[id] = []
            self.msg_queue[id].append(data)
        pr.disable()
        end = time.time()
        print("read msg took %.4f seconds" % (end-start))

        con_info, msg_count = '', 0
        byte_data = bytearray()
        for i in self.msg_queue.keys():
            con_info += str(i) + ' '
            msgs = self.msg_queue[i]
            for msg in msgs:
                msg_count += 1
                byte_data.extend(msg)
        con_info += '/' + str(end-start) + '/' + str(msg_count)
        print("send {} {} messages from from {} to {}, size: {:.4f} KB".format(msg_count, topics, start_time, end_time, len(byte_data) / 1024))
        
        self.msg_queue.clear()
        self.socket.send_multipart([con_info.encode(), byte_data])

    # in this mode bagname is a dir containing splited bag files
    def read_ros1_latest_msg(self, topics, time_len):
        start = time.time()
        bag_list = os.listdir(self.bag_name)
        bag_list = [x for x in bag_list if not x.endswith('.active')]
        bag_list = natsorted(bag_list)
        
        if time_len <= self.duration:
            bag_path = self.bag_name + '/' + bag_list[-1]
            print(bag_path)
            # pr.enable()
            bag = rosbag.Bag(bag_path)
            end_time = bag.get_end_time()
            start_time = rospy.Time(end_time - time_len)
            end_time = rospy.Time(end_time)
            for topic, m, t in bag.read_messages(topics, start_time, end_time, raw=True):
                id, data = m[0], m[1]
                if id not in self.msg_queue.keys():
                    self.msg_queue[id] = []
                self.msg_queue[id].append(data)
            # pr.disable()
            end = time.time()
            print("read msg took %.4f seconds" % (end-start))

            con_info, msg_count = '', 0
            byte_data = bytearray()
            for i in self.msg_queue.keys():
                con_info += str(i) + ' '
                msgs = self.msg_queue[i]
                for msg in msgs:
                    msg_count += 1
                    byte_data.extend(msg)
            con_info += '/' + str(end-start) + '/' + str(msg_count)
            print("send {} {} messages from last {} seconds, size: {:.4f} KB".format(msg_count, topics, time_len, len(byte_data) / 1024))

            self.socket.send_multipart([con_info.encode(), byte_data])
            self.msg_queue.clear()
        else:
            bag_path = self.bag_name + '/' + bag_list[-1]
            # pr.enable()

            bag = rosbag.Bag(bag_path)
            end_time = bag.get_end_time()
            start_time = end_time - time_len
            ranged_bag, ranged_bag_path = [bag], [bag_path]

            # Skip the last bag added before
            for i in range(len(bag_list) - 2, -1, -1):
                bag_path = f"{self.bag_name}/{bag_list[i]}"
                this_bag = rosbag.Bag(bag_path)
                ranged_bag.append(this_bag)
                ranged_bag_path.append(bag_path)
                this_bag_start_time = this_bag.get_start_time()
                if this_bag_start_time < start_time:
                    break

            start_time = rospy.Time(end_time - time_len)
            end_time = rospy.Time(end_time)
            for bag in ranged_bag:
                for topic, m, t in bag.read_messages(topics, start_time, end_time, raw=True):
                    id, data = m[0], m[1]
                    if id not in self.msg_queue.keys():
                        self.msg_queue[id] = []
                    self.msg_queue[id].append(data)
            # pr.disable()
            end = time.time()
            print("read msg took %.4f seconds" % (end-start))
    
            con_info, msg_count = '', 0
            byte_data = bytearray()
            for i in self.msg_queue.keys():
                con_info += str(i) + ' '
                msgs = self.msg_queue[i]
                for msg in msgs:
                    msg_count += 1
                    byte_data.extend(msg)
            con_info += '/' + str(end-start) + '/' + str(msg_count)
            print("send {} {} messages from last {} seconds, size: {:.4f} KB".format(msg_count, topics, time_len, len(byte_data) / 1024))
    
            self.socket.send_multipart([con_info.encode(), byte_data])
            self.msg_queue.clear()   
                
    def read_ros1_ranged_msg(self, topics, start_time, end_time):
        start = time.time()
        bag_list = os.listdir(self.bag_name)
        bag_list = [x for x in bag_list if not x.endswith('.active')]
        bag_list = natsorted(bag_list)
        print(start_time, end_time)
        ranged_bag = []

        for bag_file in bag_list:
            bag_path = self.bag_name + '/' + bag_file
            try:
                bag = rosbag.Bag(bag_path)
                bag_start_time = bag.get_start_time()
                break
            except rosbag.ROSBagException as e:
                if str(e) == 'Bag contains no message':
                    continue
                else:
                    raise
        else:
            raise ROSBagException('No valid bag file found in the list')
        
        query_start_time = rospy.Time(bag_start_time + start_time)
        query_stop_time = rospy.Time(bag_start_time + end_time)

        indices = range(int(start_time // self.duration), int(end_time // self.duration + 1))
        ranged_bag = [bag_list[i] for i in indices]
        print(ranged_bag)

        for bag in ranged_bag:
            bag_path = self.bag_name + '/' + bag
            bag = rosbag.Bag(bag_path)
            for topic, m, t in bag.read_messages(topics, query_start_time, query_stop_time, raw=True):
                id, data = m[0], m[1]
                if id not in self.msg_queue.keys():
                    self.msg_queue[id] = []
                timestamp = str(t.secs) + '.' + str(t.nsecs)[0:2]
                self.msg_queue[id].append(data)
        end = time.time()
        print("read msg took %.4f seconds" % (end-start))

        con_info, msg_count = '', 0
        byte_data = bytearray()
        for i in self.msg_queue.keys():
            con_info += str(i) + ' '
            msgs = self.msg_queue[i]
            for msg in msgs:
                msg_count += 1
                byte_data.extend(msg)
        con_info += '/' + str(end-start) + '/' + str(msg_count)
        print("send {} {} messages from from {} to {}, size: {:.4f} KB".format(msg_count, topics, start_time, end_time, len(byte_data) / 1024))

        self.socket.send_multipart([con_info.encode(), byte_data])
        self.msg_queue.clear()
    
    def read_auto_msg(self, topics, time_len):
        bag = rosbag.Bag(self.bag_name, 'rosfs')
        topic_sizes = self.get_msg_size(bag)

        time_len = 5
        max_data = 1024**3
        if self.wifi_on:
            if len(self.wifi_monitor.signal_levels) > 0:
                cur_signal = -self.wifi_monitor.signal_levels[-1]
            else:
                cur_signal = 52.0

            max_data = 100 / 8  * MB2B * (25.0 / cur_signal * 1.5)
            print("dBm {} estimated bw: {} MB/s".format(cur_signal, max_data/MB2B))
            max_data = int(max_data)
        else:
            msg_count = self.bandwidth / topic_sizes[topics[0]]

        start = time.time()
        data_len = 0
        for topic, m, t in bag.read_latest_messages(topics, time_len, raw=True):
            if data_len > max_data:
                break
            id, data = m[0], m[1]
            if id not in self.msg_queue.keys():
                self.msg_queue[id] = []
            self.msg_queue[id].append(data)
            data_len += len(data)
        end = time.time()
        
        con_info, msg_count = '', 0
        byte_data = bytearray()
        for i in self.msg_queue.keys():
            con_info += str(i) + ' '
            msgs = self.msg_queue[i]
            for msg in msgs:
                msg_count += 1
                byte_data.extend(msg)
        con_info += '/' + str(end-start) + '/' + str(msg_count)
        print("send {} {} messages, size: {:.4f} KB".format(msg_count, topics, len(byte_data) / 1024))
        self.socket.send_multipart([con_info.encode(), byte_data])
        self.msg_queue.clear()

class ContainerClient(object):
    def __init__(self, host, port, dump=False):
        self.socket = zmq.Context().socket(zmq.REQ)
        # self.addr = f"tcp://{host}:{port}"
        self.addr = "tcp://"+ host + ":" + port
        self.socket.connect(self.addr)
        self.bag_name = None
        self.last_req = None
        self.req_queue = []
        self.recv_queue = []
        self.req_cmds = ['kill', 'q', 'qt', 'qa']
        # print("Connecting to {}".format(self.addr))
    
    def check_topic_names(self, topics):
        for t in topics:
            if t not in bag2topic.keys():
                return False
        return True

    def start_req(self, path):
        self.cmd_list = []
        self.rt_result = []
        with open(path, 'r') as f:
            self.cmd_list = f.read().splitlines()
        self.send_time = time.time()
        self.socket.send(self.cmd_list[0].encode())
        header_bytes = self.socket.recv()
        count = 1
        while True:
            cur_cmd = self.cmd_list[count]
            tokens = cur_cmd.split()
            if tokens[0] not in self.req_cmds:
                break
            
            if tokens[0] == 'kill':
                self.socket.send(cur_cmd.encode())
                self.socket.recv()
                break

            if time.time() - self.send_time >= 1:
                print(cur_cmd)
                self.socket.send(cur_cmd.encode())
                self.send_time = time.time()
                count += 1
                con_id, byte_data = self.socket.recv_multipart()
                recv_time = time.time()
                rtt = recv_time - self.send_time
                # self.rt_result.append((rtt, len(byte_data), con_id.decode()))
                print("rtt: %.4f s" % rtt)
            
            # Relax and wait for next command
            time.sleep(0.1)

        # timestr = time.strftime("%Y%m%d-%H%M")
        # with open(f"res_{timestr}.json", 'w') as f:
        #     json.dump(self.rt_result, f, indent=2)

    def talking(self):
        print("Input \"BAG bag_name\" to start querying")
        while True:
            req = input()
            if req == "":
                continue
            tokens = req.split()
            cmd = tokens[0]
            if self.bag_name and cmd not in self.req_cmds:
                print("Invalid request commands")
                continue
            
            elif cmd == 'q':
                if len(tokens) <= 1 or tokens[-1].isdigit() == False:
                    print("Usage: q topic1 topic2 ... time_len")
                    continue
                if self.check_topic_names(tokens[1:-1]) == False:
                    print("requesting topic name not found")
                    continue
            
            elif cmd == 'qt':
                if len(tokens) <= 1:
                    print("Usage: qt topic1 topic2 ... start_time end_time")
                    continue
                if int(tokens[-2]) > int(tokens[-1]):
                    print("qt: start_time should be smaller than end_time")
                    continue
                if self.check_topic_names(tokens[1:-2]) == False:
                    print("requesting topic name not found")
                    continue
            
            elif cmd == 'qa':
                if len(tokens) <= 1:
                    print("Usage: qa topic1 topic2 ...")
                    continue
                if self.check_topic_names(tokens[1:]) == False:
                    print("requesting topic name not found")
                    continue
            
            elif cmd == 'kill':
                self.bag_name = None
            
            elif cmd == 'close':
                self.socket.close()
                break
                
            self.socket.send(req.encode())
            self.send_time = time.time()
            
            if cmd == "BAG":
                header_bytes = self.socket.recv()
                if len(tokens) > 1:
                    self.bag_name = tokens[1]
                    self.req_queue.append(tokens[0])
            
            elif self.bag_name:
                if not self.req_queue or self.req_queue[len(self.req_queue)-1] != 'BAG':
                    print(self.socket.recv().decode())
                else:
                    con_id, byte_data = self.socket.recv_multipart()
                    recv_time = time.time()
                    response_time = recv_time - self.send_time
                    print("rtt: %.4f s" % response_time)
            else:
                self.socket.recv()

if __name__=='__main__':

    parser = argparse.ArgumentParser()
    parser.add_argument("--mode", "-M", type=str, nargs=1, help="server or client mode", default='client')
    parser.add_argument("--host", "-H", type=str, nargs=1, help="host address", required=True)
    parser.add_argument("--port", "-P", type=int, nargs=1, help="port", required=True)
    parser.add_argument("--wifi", type=str, nargs=1, help="open wifi monitoring on server side, please specify wlp device name")
    parser.add_argument("--wifi-sim", type=str, nargs=1, help="open wifi monitoring on server side, load signal levels from file")
    parser.add_argument("--test", action='store_true', help="testing mode", default=False)
    parser.add_argument("--path", type=str, help="backend path of bags on server",required=True)
    parser.add_argument("--auto", type=str, nargs=1, help="load requests from file and send to servert automatically", default=False)
    parser.add_argument("--bag", type=str, help="bag format", default="rosfs")
    parser.add_argument("--duration", type=float, help="bag splited duration", default=10)

    args = parser.parse_args()

    mode, host, port = args.mode[0], args.host[0], str(args.port[0])

    if mode == "server":
        try:
            server = ContainerServer(host, port, args.wifi, args.path, 
                                     duration=args.duration, bag_format=args.bag)
            if args.wifi:
                monitor = WifiMonitor(10, wlp_name=args.wifi[0])
                monitor.start_monitoring()
                server.get_wifi_monitor(monitor)
            elif args.wifi_sim:
                server.wifi_on = True
                monitor = WifiMonitor(10, simulate=args.wifi_sim[0])
                monitor.start_monitoring()
                server.get_wifi_monitor(monitor)
            
            server.listening()
        except KeyboardInterrupt:
            sys.exit(1)

        pr.dump_stats("/root/ros1-bags/pipeline.prof")

    elif mode == "client":
        try:
            client = ContainerClient(host, port)
            if args.auto == False:
                client.talking()
            else:
                client.start_req(args.auto[0])
        except KeyboardInterrupt:
            sys.exit(1)
    
    else:
        sys.exit("no such mode")
