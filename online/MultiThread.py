import rosbag
import rospy

import time,threading,sys

tps =  [
        "/camera/infra1/image_rect_raw/compressed"
]

format = ['ROSfs']

class ROSfs_Bag:
    def __init__(self,bak):
        self.reader = rosbag.Bag(bak,'rosfs')
    
    def read_latest_msg(self,topics,time_len):
        ets = self.reader.get_end_time()
        sts = rospy.Time(max(0,ets-time_len))
        ets = rospy.Time(ets)
        
        st = time.time()
        etf = False
        for _ in self.reader.read_messages(topics,sts,ets):
            if not etf:
                et = time.time()
                print(et-st)
                break     
        

rtype = [ROSfs_Bag]
num_threads = [1,2,4,8,16]
def task(rt,backend,format,topics,time_len):
    reader = rt(backend)
    reader.read_latest_msg(topics,time_len)
    
if __name__=="__main__":
    print('Usage: python3 MultiThread.py /path/to/your/rosfs-container')
    backend = sys.argv[1]
    tl = 1
    threads = None
    for nt in num_threads:
        for r,f in zip(rtype,format):
            threads = []
            print(f)
            for _ in range(nt):
                thread = threading.Thread(target=task,args=(r,backend,f,tps,tl))
                threads.append(thread)
                thread.start()
            
            for t in threads:
                t.join()