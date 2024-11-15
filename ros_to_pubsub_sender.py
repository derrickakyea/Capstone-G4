from http import client

import rclpy
from rclpy.node import Node
from sensor_msgs.msg import JointState
import threading
import pandas as pd
import json
import datetime

from google.oauth2 import service_account

key_path = "key1.json"
credentials = service_account.Credentials.from_service_account_file(
    filename=key_path, scopes=["https://www.googleapis.com/auth/cloud-platform"],
)

from google.cloud import pubsub_v1

publisher = pubsub_v1.PublisherClient(credentials=credentials)
topic_path = publisher.topic_path("capstone-project-440014","Robotic-arm-data")

# import tensorflow as tf
# from keras.utils.vis_utils import plot_model

import numpy as np
import json_numpy
json_numpy.patch()





class JointStateSubscriber(Node):

    def __init__(self):
        super().__init__('JointState_subscriber')
        self.lock = threading.Lock()
        self.positions = []
        self.JointState_subscriber = self.create_subscription(
            JointState, 'joint_states', self.listener_callback, 10)

    def listener_callback(self, msg):
        #self.get_logger().info('Position: {0}'.format(msg.position))
        self.lock.acquire()
        self.positions.append(msg.position)
        #self.positions = self.positions[-10:]
        nump = np.asanyarray(self.positions)
        timestamp= datetime.datetime.now().isoformat()
        #df = pd.DataFrame(self.positions)
        json_data = json.dumps({'timestamp':timestamp,'numpy':json.dumps(nump)})
        publisher.publish(topic_path,json_data.encode("utf-8"))
        print("Position: ", json.loads(json_data))

        self.lock.release()
        
rclpy.init()
node = JointStateSubscriber()
rclpy.spin(node)
node.destroy_node()
rclpy.shutdown()
