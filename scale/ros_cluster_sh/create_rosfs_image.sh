#!/bin/bash

if ! docker network ls | grep -q ros_cluster_network; then
    docker network create ros_cluster_network
fi

docker build -t ros_robot_node .