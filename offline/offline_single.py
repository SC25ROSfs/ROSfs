import rosbag
import time,sys

if __name__ == "__main__":
    print('Usage: python3 offline_single.py /path/to/1st-rosfs-container /path/to/2nd-rosfs-container ...')
    tps = [
        "/camera/depth/image",
        "/camera/rgb/image_color",
        "/imu",
        "/cortex_marker_array",
        "/camera/depth/camera_info",
        "/camera/rgb/camera_info"
    ]
    
    backends = sys.argv[1:]
    
    def offline_query_time(topic:str,backend:str)->float:
        rosfs_bag = rosbag.Bag(backend,"rosfs")
        st = time.time()
        for _,_,_ in rosfs_bag.read_messages(topic):
            pass
        return time.time()-st
    
    for bak in backends:
        print(bak)
        for tp in tps:
            print(offline_query_time(tp,bak))