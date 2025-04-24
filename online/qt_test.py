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
    print('Usage: python3 qt_test.py /absolute_path/to/your/bag(or rosfs container)')
    print('Make Sure your bag has recorded more than 12s msg data since we\'use a 0-12s random offset')
    bak = sys.argv[1]
    rosfs_bag = rosbag.Bag(bak,'rosfs')
    for g in groups:
        cet = rosfs_bag.get_end_time()
        qduration = random.randint(0,12)
        qst,qet = cet-qduration,cet
        st = time.time()
        cnt = 0
        for _,_,_ in rosfs_bag.read_messages(g,qst,qet):
            cnt += 1
        et = time.time()
        print(et-st,cnt)