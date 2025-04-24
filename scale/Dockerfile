# It's necessary to build the rosfs image first
From rosfs

RUN apt-get update && apt-get install -y \
    python3.8-venv \
    lsof \
    trickle

WORKDIR /root/ros_catkin_ws