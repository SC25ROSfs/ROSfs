#!/bin/bash

source config.sh

GROUP_SIZE=5
TOPIC_LISTS=(
    "/davis/left/camera_info /davis/left/events /davis/left/image_raw /davis/left/imu /davis/right/events /davis/right/imu /visensor/cust_imu /visensor/imu /visensor/left/camera_info /visensor/left/image_raw /visensor/right/camera_info /visensor/right/image_raw"
    "/davis/left/image_raw /davis/right/imu /visensor/left/camera_info /visensor/left/image_raw /visensor/right/camera_info /velodyne_point_cloud"
    "/davis/left/camera_info /davis/left/image_raw /davis/left/imu /davis/right/imu"
    "/velodyne_point_cloud /visensor/left/camera_info /visensor/left/image_raw /visensor/right/camera_info /visensor/right/image_raw"
)

play_record() {
    local i=$1
    local container_name="robot_node_$i"
    local group_id=$(( (i - 1) / GROUP_SIZE ))

    echo "Executing commands in container: $container_name"
    docker exec "$container_name" bash -c "source /root/ros_catkin_ws/devel/setup.bash && roscore" > /dev/null 2>&1 &
    sleep 1

    local record_topics=${TOPIC_LISTS[$group_id]}
    docker exec "$container_name" bash -c "source /root/ros_catkin_ws/devel/setup.bash && rosbag record $record_topics --rosfs --duration=30 -O /data/rosfs_robot_node_$i" > "/dev/null" 2>&1 &
    sleep 2

    docker exec "$container_name" bash -c "source /root/ros_catkin_ws/devel/setup.bash && rosbag play /data/outdoor.bag --duration=30" > /dev/null 2>&1
}

for i in $(seq 1 $cluster_size); do
    play_record "$i" &
done

wait

echo "Done"