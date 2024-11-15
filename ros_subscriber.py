from http import client

import rclpy
from rclpy.node import Node
from sensor_msgs.msg import JointState
import threading
import pandas as pd
import json

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
        self.positions = self.positions[-10:]
        nump = np.asanyarray(self.positions)
        #df = pd.DataFrame(self.positions)
        json_data = json.dumps(nump)
        print("Position: ", json.loads(json_data))
        self.lock.release()
        
rclpy.init()
node = JointStateSubscriber()
rclpy.spin(node)
node.destroy_node()
rclpy.shutdown()
