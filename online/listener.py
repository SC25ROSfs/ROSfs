#!/usr/bin/python3
import sys
import json
import os
import time
import rospy
import rosbag
import threading
from threading import Event

from std_msgs.msg import String
from sensor_msgs.msg import Image
from sensor_msgs.msg import CompressedImage
from sensor_msgs.msg import CameraInfo
from sensor_msgs.msg import Imu
from sensor_msgs.msg import PointCloud2

TOPIC_TYPE = {
                'image': Image,
                'image1': Image,
                'image2': Image,
                'image1c': CompressedImage,
                'image2c': CompressedImage,
                'imu': Imu,
                'info': CameraInfo,
                'str': String,
                'h_laser': PointCloud2
            }

with open("./topic_abbr.jsontmp", 'r') as f:
    bag2topic = json.load(f)

is_first_msg = False
add_thread = None

stop_event = Event()

def stop_subscriber(event):
    subscriber.unregister()
    rospy.loginfo("Subscriber stopped.")
    timer.shutdown()
    stop_event.set()

# start subscribing a specific topic
def listener(msg_type, topic_abbr, time_len):
    def callback(data):
        global is_first_msg
        if is_first_msg == False:
            print(f"1st msg latency: {time.time() - start_time: .4f} s")
            is_first_msg = True
        pub_time = data.header.stamp.to_sec()
        # latency = (cur_time - pub_time) * 1000
        rospy.loginfo(f"msg {pub_time}")

    topic = bag2topic[topic_abbr]
    rospy.init_node('listener', anonymous=True)
    print(f"subscribe to topic {topic}")
    global subscriber
    subscriber = rospy.Subscriber(topic, msg_type, callback)
    global timer
    timer = rospy.Timer(rospy.Duration(time_len), stop_subscriber)

def talking(topic_abbr, time_len):
    global is_first_msg
    is_first_msg = False
    msg_type = TOPIC_TYPE[topic_abbr]
    global start_time
    start_time = time.time()
    listener(msg_type, topic_abbr, time_len)
    stop_event.wait()

if __name__ == '__main__':
    if len(sys.argv) < 2:
        sys.exit("Usage: python3 listener.py ros_master_host topic_abbr time_len")
    master_url = "http://{}:11311".format(sys.argv[1])
    topic_abbr = sys.argv[2]
    time_len = int(sys.argv[3])
    os.environ['ROS_MASTER_URI'] = master_url
    print("\"q topic_abbr time_len\" to start subscribe a specific topic for time_len seconds")
    
    talking(topic_abbr, time_len)
