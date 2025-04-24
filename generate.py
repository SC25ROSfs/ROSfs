import sys
import os
import rosbag

def extract_and_write_messages(input_bag_path, output_bag_path):
    with rosbag.Bag(input_bag_path, 'r') as input_bag:
        with rosbag.Bag(output_bag_path, 'w') as output_bag:
            connections = input_bag.get_type_and_topic_info()[1]
            for topic in connections:
                for _, msg, t, connection_header in input_bag.read_messages(topics=[topic], return_connection_header=True):
                    output_bag.write(topic, msg, t, connection_header=connection_header)
                    break 

if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: python script.py <input_bag_path> <output_bag_path>")
        sys.exit(1)

    input_bag_path = sys.argv[1]
    output_bag_path = sys.argv[2]

    if not os.path.exists(input_bag_path):
        print(f"Error: Input bag file '{input_bag_path}' does not exist.")
        sys.exit(1)

    extract_and_write_messages(input_bag_path, output_bag_path)
    print(f"Messages extracted and written to {output_bag_path}")