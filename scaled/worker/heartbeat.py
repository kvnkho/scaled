import threading
import time

import psutil
import zmq

from scaled.utility.zmq_config import ZMQConfig
from scaled.protocol.python.message import Heartbeat
from scaled.protocol.python.objects import MessageType


class WorkerHeartbeat(threading.Thread):
    def __init__(self, address: ZMQConfig, worker_identity: bytes, interval: int, stop_event: threading.Event):
        threading.Thread.__init__(self)

        self._address = address
        self._worker_identity = worker_identity
        self._interval = interval
        self._stop_event = stop_event

        self._context = zmq.Context()
        self._socket = self._context.socket(zmq.DEALER)
        self._socket.setsockopt(zmq.LINGER, 0)
        self._socket.setsockopt(zmq.IDENTITY, self._worker_identity + b"|HB")
        self._socket.connect(self._address.to_address())

        self.start()

    def run(self) -> None:
        try:
            while not self._stop_event.is_set():
                time.sleep(self._interval)
                self._socket.send_multipart(
                    [
                        MessageType.Heartbeat.value,
                        *Heartbeat(self._worker_identity, psutil.cpu_percent() / 100).serialize(),
                    ]
                )
        finally:
            self._socket.close()
