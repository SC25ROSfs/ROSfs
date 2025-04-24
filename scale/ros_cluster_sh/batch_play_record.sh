#!/bin/bash

source config.sh

play_record() {
    local i=$1
    local container_name="robot_node_$i"

    echo "Executing commands in container: $container_name"
    docker exec "$container_name" bash -c "source /root/ros_catkin_ws/devel/setup.bash && roscore" > /dev/null 2>&1 &
    sleep 1

    docker exec "$container_name" bash -c "source /root/ros_catkin_ws/devel/setup.bash && rosbag record -a --rosfs --duration=30 -O /data/rosfs_robot_node_$i" > /dev/null 2>&1 &
    sleep 2

    docker exec "$container_name" bash -c "source /root/ros_catkin_ws/devel/setup.bash && rosbag play /data/car.bag --duration=30" > /dev/null 2>&1
}

for i in $(seq 1 $cluster_size); do
    play_record "$i" &
done

wait

echo "Done"