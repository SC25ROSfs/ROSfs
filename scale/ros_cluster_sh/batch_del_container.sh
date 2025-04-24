#!/bin/bash

source config.sh

stop_and_remove_container() {
    local container_name="robot_node_$1"
    echo "Stopping and removing container: $container_name"
    
    docker stop "$container_name" &>/dev/null
    if [ $? -eq 0 ]; then
        echo "Container $container_name stopped successfully."
    else
        echo "Failed to stop container $container_name" >&2
    fi

    docker rm "$container_name" &>/dev/null
    if [ $? -eq 0 ]; then
        echo "Container $container_name removed successfully."
    else
        echo "Failed to remove container $container_name" >&2
    fi
}

for i in $(seq 1 $cluster_size); do
    stop_and_remove_container "$i" &
done

wait

echo "All containers have been stopped and removed."