#!/bin/bash

if [ -z "$1" ]; then
    echo "Usage: $0 <python_script_name>"
    exit 1
fi

source config.sh

PYTHON_SCRIPT="$1"
BASE_DIR="/data/scripts/test"

process_container() {
    local container_name="robot_node_$1"
    sudo docker exec "$container_name" bash -c "trickled -d $bandwidth -u $bandwidth -s &"

    sleep 2

    sudo docker exec "$container_name" bash -c "
        source /root/ros_catkin_ws/devel/setup.bash &&
        source /data/sc_venv/bin/activate &&
        cd $BASE_DIR &&
        ulimit -S -v unlimited &&
        trickle -d $bandwidth -u $bandwidth python3 $PYTHON_SCRIPT
    " > log_$i.txt 2>&1
}

for i in $(seq 1 $cluster_size); do
    process_container "$i" &
done

wait

echo "All containers processed."