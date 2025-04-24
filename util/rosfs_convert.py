import sys
import rosbag
import time
import shutil

def convert(src:str,dst:str):
    print("ROSfs converting")
    st = time.time()
    try:
        rosbag.rosfs_timekv.create(target)
        
        rosfs_bag = rosbag.Bag(target,"rosfs")
        bag = rosbag.Bag(src)
        
        rst = time.time()
        topics,msgs,ts,connection_headers = [],[],[],[]
        for topic,msg,t,conn_header in bag.read_messages(return_connection_header=True):
            for k,v in conn_header.items():
                if isinstance(v,bytes):
                    conn_header[k] = v.decode('utf-8')
            topics.append(topic)
            msgs.append(msg)
            ts.append(t)
            connection_headers.append(conn_header)
        ret = time.time()
        print(f"ROSfs convertion pre read complete,time use:{ret-rst}")
        rosfs_bag.rosfs_batch_write(topics,msgs,ts,connection_headers=connection_headers)
    except Exception as e:
        print(f"Exception while converting:{e}")
        shutil.rmtree(target)
        exit(130)
    finally:
        bag.close()
    et = time.time()
    print(f"ROSfs convertion complete,time use:{et-st}")

if __name__ == "__main__":
    print('Usage: python3 rosfs_convert.py /path/to/ros1-bag /path/to/target/rosfs-container')
    print('Make sure you\'ve sourced the setup.bash within the rosfs catkin workspace')
    
    args = sys.argv
    
    src = args[1]
    target = args[2]
    
    convert(src,target)