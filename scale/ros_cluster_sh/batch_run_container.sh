#!/bin/bash

source config.sh

start_container() {
    local i=$1
    local container_name="robot_node_$i"

    echo "Starting container: $container_name"
    docker run -d --name "$container_name" --network ros_cluster_network -v "$shared_backend:/data" ros_robot_node tail -f /dev/null
}

for i in $(seq 1 $cluster_size); do
    start_container "$i" &
done

wait