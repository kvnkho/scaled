import logging
import sys 
import asyncio
from typing import List
import threading

import zmq

from scaled.utility.zmq_config import ZMQConfig
from scaled.io.async_connector import AsyncConnector
from scaled.io.sync_connector import SyncConnector
from scaled.protocol.python.message import MessageType, MessageVariant
from scaled.protocol.python.message import TaskLog
from scaled.protocol.python.serializer.default import DefaultSerializer

class NetworkLogHandler(logging.Handler):
    """
    The NetworkLogHandler is responsible for sending task
    logs from the workers to the scheduler. The scheduler
    can then forward it to the workers.

    The purpose of using a Handler is so that it can be
    attached to the existing logger of a task without
    code change from the user.
    """
    def __init__(self, network_log_connector: SyncConnector):
        self.__network_log_connector = network_log_connector
        self._serializer = DefaultSerializer()
        self.setFormatter(logging.Formatter('%(asctime)s %(levelname)s %(message)s'))
        super().__init__()

    def emit(self, record):
        self.__network_log_connector.send_immediately(
            MessageType.TaskLog,
            self._serializer.serialize_result(TaskLog(record.msg, record.levelname)),
        )



class NetworkLogForwarder:
    """
    The NetworkLogForwarder is intended to live on the Scheduler. It takes
    all the logs from the workers emitted by the NetworkLogHandler and passes 
    them back to the NetworkLogPublisher where they can be consumed and handled (stdout, file, etc)

    The terminology here follows the ZMQ Forwarder Architecture:
    https://learning-0mq-with-pyzmq.readthedocs.io/en/latest/pyzmq/devices/forwarder.html
    """
    def __init__(self, 
                 frontend_connector: AsyncConnector = None,
                 backend_connector: AsyncConnector = None):
        self._frontend_connector = frontend_connector
        self._backend_connector = backend_connector
        if self._frontend_connector:
            self._frontend_connector._socket.setsockopt(zmq.SUBSCRIBE, b"")


    async def routine(self):
        """
        The goal is to just forward so we don't need to deserialize and serialize
        """
        if self._frontend_connector:
            frames = await self._frontend_connector._socket.recv_multipart()
            await self._backend_connector._socket.send_multipart(frames, copy=False)


class NetworkLogPublisher:
    """
    The NetworkLogPublisher is responsible for handling the worker logs
    on the client side. It can be configured to write logs to file
    or simply stream to stdout.
    """
    def __init__(self, 
                 log_address: str, 
                 context: zmq.Context,
                 handlers: List[logging.Handler] = None):
        self._internal_connector = SyncConnector(
            stop_event=threading.Event(),
            prefix="CL",
            context=context,
            socket_type=zmq.SUB,
            bind_or_connect="connect",
            address=ZMQConfig.from_string(log_address),
            callback=self.__on_receive,
            exit_callback=None,
            daemonic=True,
        )
        self._internal_connector._socket.setsockopt(zmq.SUBSCRIBE, b"")
        self.logger = logging.getLogger()
        self.handlers = handlers or [logging.StreamHandler(sys.stdout)]
        for handler in self.handlers:
            self.logger.addHandler(handler)

    def __on_receive(self, message_type: MessageType, log: TaskLog):
        """
        Parses the TaskLog object and outputs it at the appropriate level
        """
        assert message_type == MessageType.TaskLog
        
        level = logging._nameToLevel[log.level]
        self.logger.log(level=level, msg=log.message)
        return

