import logging
import sys 

from scaled.utility.zmq_config import ZMQConfig
from scaled.io.sync_connector import SyncConnector
from scaled.protocol.python.message import MessageType
from scaled.protocol.python.message import TaskLog
from scaled.protocol.python.serializer.default import DefaultSerializer

class NetworkLogHandler(logging.Handler):
    """
    The NetworkHandler is responsible for sending task
    logs from the workers to the scheduler. The scheduler
    can then forward it to the workers.

    The purpose of using a Handler is so that it can be
    attached to the existing logger of a task without
    code change from the user.
    """
    def __init__(self, network_log_connector: SyncConnector):
        self._internal_connector = network_log_connector
        self._serializer = DefaultSerializer()
        self.setFormatter(logging.Formatter('%(asctime)s %(levelname)s %(message)s'))
        super.__init__()

    def emit(self, record):
        self._internal_connector.send_immediately(
            MessageType.TaskLog,
            TaskLog(self._serializer.serialize_results(record.msg)),
        )


class NetworkLogForwarder():
    """
    The Forwarder is intended to live on the Scheduler. It takes
    all the logs from the workers and passes them back to the  Client
    where they can be consumed and handled (stdout, file, etc)
    """
