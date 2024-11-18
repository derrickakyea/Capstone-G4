import datetime

from google.cloud import pubsub_v1
import json

import tensorflow as tf
# from keras.utils.vis_utils import plot_model
import threading
import pandas as pd
import numpy as np
from keras._tf_keras.keras.metrics import mae
import json_numpy
from google.cloud import bigquery

json_numpy.patch()

from google.cloud import storage
from google.oauth2 import service_account

key_path = "key1.json"
credentials = service_account.Credentials.from_service_account_file(
    filename=key_path, scopes=["https://www.googleapis.com/auth/cloud-platform"],
)

project_name = "capstone-project-440014"

# BigQuery client
bigQ_client = bigquery.Client(credentials=credentials, project=credentials.project_id,)
table_id = f"{project_name}.ros_data.grafana"

subscriber = pubsub_v1.SubscriberClient(credentials=credentials)
subscription_path = subscriber.subscription_path(project_name, "Ros-sub")





#Loading the autoencoder model
storage_client = storage.Client(credentials=credentials, project=credentials.project_id,)
bucket = storage_client.bucket("ros2")
blob = bucket.blob("Model 18-Nov-2024 00-33-20.h5")
model_file_temp = blob.download_to_filename("Model 18-Nov-2024 00-33-20.h5")
model = tf.keras.models.load_model("Model 18-Nov-2024 00-33-20.h5", custom_objects={"mae": mae})
if model is not None:
    print("Got the model")
else:
    print("no model found")

row = []
def callback(message: pubsub_v1.subscriber.message.Message) -> None:
    #print(f"Rece {json.loads(message.data)}")
    if (message) is None:
        print("not received")
    in_sample = json.loads(message.data)
    numpy_array = json.loads(in_sample["numpy"])
    in_data = numpy_array.reshape(numpy_array.shape[0], 1, numpy_array.shape[1])
    out_data = model.predict(in_data, verbose=0)
    timestamp = in_sample["timestamp"]
    difference = in_data - out_data
    loss_mae = np.mean(np.abs(difference), axis=1)
    anomalies = loss_mae > 0.021026134 # Flatten to 1D if needed
    print(f"type: {type(anomalies)}")
    print(f"anomalies: {anomalies}")

    for i, (positions, is_anomaly_row) in enumerate(zip(numpy_array, anomalies)):
        row_ = {
            "timestamp": str(datetime.datetime.now()),
            "should_lift": positions[0],
            "elbow": positions[1],
            "wrist1": positions[2],
            "wrist2": positions[3],
            "wrist3": positions[4],
            "shoulder_pan": positions[5],
            "anomaly_should_lift": bool(is_anomaly_row[0]),
            "anomaly_elbow": bool(is_anomaly_row[1]),
            "anomaly_wrist1": bool(is_anomaly_row[2]),
            "anomaly_wrist2": bool(is_anomaly_row[3]),
            "anomaly_wrist3": bool(is_anomaly_row[4]),
            "anomaly_shoulder_pan": bool(is_anomaly_row[5]),
        }
        row.append(row_)

    # print(f"Processed rows for database: {row}")
    print(f"Anomalies detected: {np.sum(anomalies)}")
    errors = bigQ_client.insert_rows_json(table_id, row)  # Make an API request.
    if errors == []:
        print("New rows have been added.")
    else:
        print("Encountered errors while inserting rows: {}".format(errors))
    message.ack()

print(row)

stream = subscriber.subscribe(subscription_path, callback=callback)
print(f"sub path - {subscription_path}")

with subscriber:
    stream.result()
