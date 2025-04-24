import rosbag
import time,sys

if __name__ == "__main__":
    print('Usage:python3 offline_time.py /path/to/your/rosfs-container')
    print('Make sure you\'ve recorded more than 120s msg data within the container')
    
    tps = [
        "/visensor/imu",
        "/davis/left/imu",
        "/visensor/left/image_raw",
        "/davis/left/image_raw"
    ]
    
    backend = sys.argv[1]
    
    step = 5
    
    def tr_query(topic:str,backend:str,start:int,end:int)->float:
        rosfs_bag = rosbag.Bag(backend,"rosfs")
        cst = rosfs_bag.get_start_time()
        st = time.time()
        for _,_,_ in rosfs_bag.read_messages(topic,cst+start,cst+end):
            pass
        return time.time()-st
    
    for tp in tps:
        print(tp)
        seq = []
        for i in range(40,121,5):
            seq.append(tr_query(tp,backend,0,i))
        print(seq)