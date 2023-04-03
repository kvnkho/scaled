from scaled.io.sync_connector import SyncConnector
import zmq
from scaled.utility.zmq_config import ZMQConfig

context = zmq.Context()
socket = context.socket(zmq.SUB)
socket.connect("tcp://127.0.0.1:3457")
socket.setsockopt(zmq.SUBSCRIBE, b"")

while True:
    frames = socket.recv_multipart()
    message_type_bytes, *payload = frames
    print(message_type_bytes)
    print(payload)
