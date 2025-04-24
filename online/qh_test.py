import rosbag
import time,sys,random

groups = [
    "/camera/infra1/image_rect_raw",
    "/camera/infra2/image_rect_raw/compressed",
    "/mavros/imu/data",
    ["/camera/infra1/image_rect_raw",
    "/camera/infra2/image_rect_raw/compressed"],
    ["/camera/infra1/image_rect_raw",
    "/camera/infra2/image_rect_raw/compressed",
    "/mavros/imu/data"]
]

if __name__ == "__main__":
    print('Usage: python3 qh_test.py /path/to/your/bag(or rosfs container)')
    print('Make Sure your bag has recorded more than 110s msg data since we\'use a 0-100s random offset')
    bak = sys.argv[1]
    rosfs_bag = rosbag.Bag(bak,'rosfs')
    for g in groups:
        # cst = rosbag.Bag('./data/near-wifi.bag').get_start_time()
        cst = rosfs_bag.get_start_time()
        est = rosfs_bag.get_end_time()
        qst,qduration = cst+random.randint(0,100),random.randint(1,8)
        qet = qst+qduration
        st = time.time()
        cnt = 0
        for _,_,_ in rosfs_bag.read_messages(g,qst,qet):
            cnt += 1
        et = time.time()
        print(et-st,cnt)