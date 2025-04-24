import argparse,zmq,rosbag,rospy
import json,time,abc,time,concurrent.futures,os
from typing import List,Optional

class AOI_Monitor:
    
    def __init__(self,bag_format:str,delta0:float=0.0):
        self.time_sequence = []
        self.delta = delta0
        self.format = bag_format
        self.start_time = time.time()
    
    
    def append_ts_sample(self,ti:float,tii:float):
        self.time_sequence.append((ti,tii))
    
    def clear_time_sequence(self):
        self.time_sequence.clear()
    
    def dump_aoi_sequence(self,target:str):
        if len(self.time_sequence)==0:
            return
        print(f"dump aoi into {target}")
        with open(target,'w',encoding='utf-8') as tf:
            data = {
                "delta": self.delta,
                "time_sequence": self.time_sequence
            }
            json.dump(data,tf,indent=4)
        self.clear_time_sequence() 

class BAGReader(abc.ABC):
    def __init__(self,bag_name:str,bag_format:str) -> None:
        self.msg_queue = {}         # {id, [(msg_data, timestamp)...]}
        self.bag_name = bag_name
        self.bag_format = bag_format
        self.st = time.time()

        print(bag_format)
        self._aoi_monitor = AOI_Monitor(self.bag_format,653.499411 if bag_format not in ['rosfs','ROSfs'] else 0.0)
        
    def get_relative_timestamp(self)->float:
        return time.time()-self.st
    
    def dump_aoi_data(self,target):
        self._aoi_monitor.dump_aoi_sequence(target)
    
    def msg_queue2buffer(self)->bytearray:
        byte_data = bytearray()
        cnt = 0
        for i in self.msg_queue.keys():
            msgs = self.msg_queue[i]
            cnt += len(msgs)
            for msg in msgs:
                byte_data.extend(msg)
        self.msg_queue.clear()
        return byte_data
    
    @abc.abstractmethod
    def open_bag(self,bag_path:str):
        raise NotImplementedError
    
    def _msg2queue(self,connection_id,raw_data,con_info:str)->str:
        if connection_id not in self.msg_queue:
            con_info += f"{connection_id} "
            self.msg_queue[connection_id] = []
        self.msg_queue[connection_id].append(raw_data)
        return con_info
        
    @abc.abstractmethod
    def read_latest_msg(self, topics, time_len):
        raise NotImplementedError
    
    @abc.abstractmethod
    def read_ranged_msg(self, topics, start_time, end_time):
        raise NotImplementedError

class ROSfsReader(BAGReader):
    def __init__(self, bag_name: str, bag_format: str) -> None:
        super().__init__(bag_name, bag_format)
        self.bag_format = 'rosfs'
        
    def open_bag(self, bag_path: str):
        try:
            bag = rosbag.Bag(self.bag_name, self.bag_format) # rosfs
            return bag
        except Exception as e:
            return e
        
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
    
    def read_latest_msg(self, topics:List[str], time_len:int):
        bag = self.open_bag('')
        if not isinstance(bag,rosbag.Bag):
            return [f'Error happens when opening {self.bag_name}:{bag}'.encode(),b'']
        start_time = bag.get_start_time()
        end_time = bag.get_end_time()
        start_time = rospy.Time(max(0,end_time - time_len))
        end_time = rospy.Time(end_time)
        
        con_info = ''
        for _, m, _ in bag.read_messages(topics, start_time, end_time, raw=True):
            ti = self.get_relative_timestamp()
            con_info = self._msg2queue(m[0],m[1],con_info)
            tii = self.get_relative_timestamp()
            self._aoi_monitor.append_ts_sample(ti=ti,tii=tii)
        return [con_info.encode(),self.msg_queue2buffer()]
    
    def read_ranged_msg(self, topics, start_time, end_time):
        bag = self.open_bag('')
        if bag is None:
            return [f'Error happens when opening {self.bag_name}'.encode(),b'']
        bag_start_time = int(bag.get_start_time())
        start_time = bag_start_time + start_time
        end_time = bag_start_time + end_time
        
        # topic,m,t
        con_info = ''
        for _, m, _ in bag.read_messages(topics, start_time, end_time, raw=True):
            ti = self.get_relative_timestamp()
            con_info = self._msg2queue(m[0],m[1],con_info)
            tii = self.get_relative_timestamp()
            self._aoi_monitor.append_ts_sample(ti=ti,tii=tii)
        return [con_info.encode(),self.msg_queue2buffer()]

class ContainerClient(object):
    def __init__(self, host, port,timeout):
        self.socket = zmq.Context().socket(zmq.REQ)
        self.host = host
        self.port = port
        self.addr = "tcp://"+ host + ":" + port
        self.socket.connect(self.addr)
        self.bag_name = None
        self.req_cmds = ['BAG', 'BAGFORMAT' , 'q', 'qt', 'qa','kill']
        self.cur_bag_format = None
        self.socket.setsockopt(zmq.RCVTIMEO,timeout*1000)

    def kill(self,cmd:str):
        try:
            self.bag_name = None
            self.socket.send(b'kill',flags=zmq.NOBLOCK)
        except zmq.ZMQError as e:
            pass
        finally:
            self.socket.close()
    
    def try_recv(self,prompt:str):
        try:
            res = self.socket.recv().decode()
        except zmq.Again:
            res = f"Time out:{prompt}"
        
        return res
    
    def try_recv_multipart(self,prompt):
        try:
            res = self.socket.recv_multipart()
        except zmq.Again:
            res = ("data recv error",f"Time out:{prompt}")
        
        return res
    
    def send_query_req(self,cur_cmd:str):
        try:
            self.socket.send(cur_cmd.encode())
            con_id, byte_data = self.try_recv_multipart(f"send query:{cur_cmd}")
            return [con_id,byte_data]
        except zmq.ZMQError as e:
            return "Your requesting server is down"
    
    def start_req(self, req_bag_path:str):
        cmd_list = []
        
        with open(req_bag_path,'r') as reqf:
            cmd_list = reqf.read().splitlines()
            
        for cmd in cmd_list:
            tokens = cmd.split()
            if tokens[0]=="kill":
                self.kill(cmd)
                break
            else:
                yield self.send_query_req(cmd)

class ContainerServer:
    def __init__(self, host, port, container_path, topic_abbr, timeout):
        self.socket = zmq.Context().socket(zmq.REP)
        self.host = host
        self.port = port
        self.addr = "tcp://"+ host + ":" + port
        self.pth = container_path
        self.socket.bind(self.addr)
        self.socket.setsockopt(zmq.RCVTIMEO,timeout*1000)

        self.reader:Optional[BAGReader] = ROSfsReader(self.pth,"rosfs")
        self.topic_abbr = topic_abbr
        self.timeout = timeout

    def q(self,tokens:List[str]):
        topics = [self.topic_abbr[t] for t in tokens[1:-1]]
        time_len = float(tokens[-1])
        res = self.reader.read_latest_msg(topics=topics,time_len=time_len)
        self.socket.send_multipart(res)
    
    def qt(self,tokens:List[str]):
        start, end = float(tokens[-2]), float(tokens[-1])
        topics = [self.topic_abbr[t] for t in tokens[1:-2]]
        res = self.reader.read_ranged_msg(topics=topics,start_time=start,end_time=end)
        self.socket.send_multipart(res)
    
    def kill(self,tokens:List[str]):
        self.socket.close()
        if self.reader is not None:
            print('Container Server killed')
            self.reader.dump_aoi_data(os.path.join(aoit,f"{self.host}_{time.time()}.aoi"))
        
    def invalid_cmd(self,tokens:List[str]):
        self.socket.send(b'Invalid command')

    def listening(self):
        while True:
            try:
                message = self.socket.recv().decode()
            except zmq.Again:
                self.kill('kill')
                break
            if message == b'':
                self.socket.send(b"")
                continue
            
            tokens = message.split()
            if tokens[0] == "kill":
                self.kill(tokens=tokens)
                break
            elif tokens[0] == 'q':
                self.q(tokens=tokens)
            elif tokens[0] == 'qt':
                self.qt(tokens=tokens)
            else:
                self.invalid_cmd(tokens=tokens)


if __name__=='__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument("--host", "-H", type=str, nargs=1, help="host address", required=True)
    parser.add_argument("--port", "-P", type=int, nargs=1, help="port", required=True)
    parser.add_argument("--path", type=str,nargs=1, help="backend path of bags on server",required=True)
    parser.add_argument("--request",type=str,nargs=1,help="request file required by container client",required=True)
    parser.add_argument("--AoItarget",type=str,nargs=1,help="dir to store the aoi files",required=True)
    parser.add_argument('--topics',type=str,nargs=1,help="configurable json file for topics and their short names in the request files",required=True)
    args = parser.parse_args()

    path, host, port = args.path[0], args.host[0], str(args.port[0])
    tpsf = args.topics[0]
    with open(tpsf,'r',encoding='utf-8') as tpscf:
        tps = json.load(tpscf)
    reqf = args.request[0]
    aoit = args.AoItarget[0]
    try:
        os.mkdir(aoit)
    except Exception as e:
        pass
    
    server = ContainerServer(host,port,path,tps,10)
    client = ContainerClient(host,port,10)
    def start_req(c:ContainerClient):
        for _ in c.start_req(reqf):
            pass
        return "Client"
    def listen(s:ContainerServer):
        s.listening()
        return "Server"
    with concurrent.futures.ThreadPoolExecutor() as executor:
        server_res = executor.submit(listen,server)
        client_res = executor.submit(start_req,client)
        
        for future in concurrent.futures.as_completed([server_res,client_res]):
            exception = future.exception()
            if exception:
                print("An error occurred:", exception)
            else:
                task = future.result()
                print(f"{task} Done")