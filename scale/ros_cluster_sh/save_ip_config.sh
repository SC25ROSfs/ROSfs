#!/bin/bash

source config.sh

get_ip_and_save() {
    local i=$1
    local container_name="robot_node_$i"
    local ip_address

    ip_address=$(docker inspect -f '{{.NetworkSettings.Networks.ros_cluster_network.IPAddress}}' "$container_name")
    if [ -n "$ip_address" ]; then
        echo "robot_node_$i:$ip_address" >> container_ips.txt
    else
        echo "Failed to get IP address for container $container_name" >&2
    fi
}

for i in $(seq 1 $cluster_size); do
    get_ip_and_save "$i" 
done

wait