# Dataset Description

You can clone the dataset from this branch and make copies to replace the process of downloading open-source datasets or manually recording datasets.

1. `do.bag`: The input dataset for the `offline_single.py` experiment in Artifact2.
2. `outdoor.bag`: The input dataset for the `offline_time.py` experiment in Artifact2 and the `server_client.py` experiment in Artifact3, also, the input dataset for the `heterogenous_robots.py` experiment in Artifact4
3. `wifi.bag`: The input dataset for the `MultiThread.py`, `start_multiple.py`, `qt_test.py`, and `qh_test.py` experiments in Artifact3.
4. `car.bag`: The input dataset for the `anti-collision.py` experiments in Artifact4
5. `generate.py`: This script is used to extract messages from different topics in our local dataset and write them to the smaller datasets in the repository.

# Copying Message Data

1. You can use the Python API of `rosbag.Bag` (version 1.15.14 in ROS Noetic) to read the existing message data and then write it to the corresponding bag file. However, you need to pay special attention to ensuring that the newly written messages have **new timestamps**! If the message timestamps are too dense, it will neither reflect the real production environment nor be conducive to reproducing the experimental results in the paper!
2. You can use the following method to copy message data:

```bash
# In one terminal
rosbag play /path/to/source.bag --loop

# In another terminal
rosbag record -a -O /path/to/new.bag
```

However, you need to check the parameters for publishing message frequency using `rosbag play -h`. If you copy message data in this way, you need to ensure that the publishing frequency of each topic is consistent with the frequency configuration in the paper.

# Dataset Sources

## 1. `do.bag`

The data is sourced from the following two projects:

- [HKUST-Aerial-Robotics/VINS-Fusion: An optimization-based multi-sensor state estimator](https://github.com/HKUST-Aerial-Robotics/VINS-Fusion)
- [Multi Vehicle Stereo Event Camera Dataset](https://daniilidis-group.github.io/mvsec/)

## 2. `outdoor.bag`

Same as `do.bag`.

## 3. `wifi.bag`

The data is sourced from the following two projects:

- [HKUST-Aerial-Robotics/VINS-Fusion: An optimization-based multi-sensor state estimator](https://github.com/HKUST-Aerial-Robotics/VINS-Fusion)
- [mavlink/mavros: MAVLink to ROS gateway with proxy for Ground Control Station](https://github.com/mavlink/mavros)

## 4. `car.bag`

The data is sourced from [nuScenes](https://www.nuscenes.org/), specifically using the following datasets:

- Mapexpansion-v1.3
- Full dataset (v1.0)-mini
- Can bus expansion

### Data Processing

To convert the nuScenes dataset into a ROS1 bag file, the following steps were taken:

1. The nuScenes mini-1.0 dataset was converted to the MCAP file format using [foxglove/nuscenes2mcap: Convert nuscenes data to mcap file format](https://github.com/foxglove/nuscenes2mcap).
2. The `mcap merge` functionality was used to combine multiple sub-MCAP files into a single file.
3. Topics compatible with ROS1 bag were selected from the merged MCAP file and written into the `car.bag` file.

Ultimately, the `car.bag` file contains 270 entries of nuScenes data.