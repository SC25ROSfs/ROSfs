FROM ubuntu:20.04

RUN apt-get update && apt-get install -y \
    curl \
    gnupg2 \
    lsb-release \
    wget

RUN curl -sSL https://raw.githubusercontent.com/ros/rosdistro/master/ros.asc | apt-key add -
RUN sh -c 'echo "deb http://packages.ros.org/ros/ubuntu $(lsb_release -sc) main" > /etc/apt/sources.list.d/ros-noetic.list'
RUN wget http://packages.ros.org/ros.key -O - | apt-key add -
RUN apt-get update && apt-get install -y python3-catkin-tools

ENV DEBIAN_FRONTEND=noninteractive
RUN apt-get update && apt-get install -y \
    git \
    python3-rosdep \
    python3-rosinstall-generator \
    python3-vcstools \
    python3-vcstool \
    build-essential

RUN rosdep init && rosdep update

RUN mkdir /root/ros_catkin_ws

WORKDIR /root/ros_catkin_ws

RUN rosinstall_generator desktop --rosdistro noetic --deps --tar > noetic-desktop.rosinstall

RUN apt-get install -y python3-vcstool && \
    mkdir ./src && \
    vcs import --input noetic-desktop.rosinstall ./src

RUN rosdep install --from-paths ./src --ignore-packages-from-source --rosdistro noetic -y

RUN catkin build --cmake-args -DCMAKE_BUILD_TYPE=Release -DPYTHON_EXECUTABLE=/usr/bin/python3

WORKDIR /root
RUN git clone -b rosbag_storage https://github.com/SC25ROSfs/ROSfs.git rosbag_storage
RUN git clone -b rosbag https://github.com/SC25ROSfs/ROSfs.git rosbag

RUN rm -r /root/ros_catkin_ws/src/ros_comm/rosbag && \
    rm -r /root/ros_catkin_ws/src/ros_comm/rosbag_storage && \
    mv /root/rosbag_storage ~/ros_catkin_ws/src/ros_comm/rosbag_storage && \
    mv /root/rosbag ~/ros_catkin_ws/src/ros_comm/rosbag

WORKDIR /root/ros_catkin_ws/src/ros_comm/rosbag
RUN catkin build --this
    
RUN echo "source /root/ros_catkin_ws/devel/setup.bash" >> ~/.bashrc

WORKDIR /root/ros_catkin_ws