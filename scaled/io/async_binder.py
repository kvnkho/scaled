import threading
import logging
import os
import socket
from collections import defaultdict
from typing import Awaitable, Callable, Dict, List, Literal, Optional

import zmq.asyncio

from scaled.utility.zmq_config import ZMQConfig
from scaled.protocol.python.message import Message, PROTOCOL
from scaled.protocol.python.objects import MessageType
from scaled.scheduler.mixins import Binder

POLLING_TIME = 1000


class AsyncBinder(Binder):
    def __init__(self, stop_event: threading.Event, prefix: str, address: ZMQConfig):
        self._stop_event = stop_event

        self._address = address
        self._identity: bytes = f"{prefix}|{socket.gethostname()}|{os.getpid()}".encode()

        self._context = zmq.asyncio.Context.instance()
        self._socket = self._context.socket(zmq.ROUTER)
        self.__set_socket_options()
        self._socket.bind(self._address.to_address())
        logging.info(f"{self.__class__.__name__}: bind to {address.to_address()}")

        self._callback: Optional[Callable[[bytes, MessageType, Message], Awaitable[None]]] = None

        self._statistics = {
            "received": defaultdict(lambda: 0),
            "sent": defaultdict(lambda: 0)
        }

    def register(self, callback: Callable[[bytes, MessageType, Message], Awaitable[None]]):
        self._callback = callback

    async def routine(self):
        count = await self._socket.poll(POLLING_TIME)
        if not count:
            return

        for _ in range(count):
            frames = await self._socket.recv_multipart()
            if not self.__is_valid_message(frames):
                continue

            source, message_type_bytes, payload = frames[0], frames[1], frames[2:]
            message_type = MessageType(message_type_bytes)
            self.__count_one("received", message_type)
            message = PROTOCOL[message_type_bytes].deserialize(payload)
            await self._callback(source, message_type, message)

    async def statistics(self) -> Dict:
        return self._statistics

    async def send(self, to: bytes, message_type: MessageType, data: Message):
        self.__count_one("sent", message_type)
        await self._socket.send_multipart([to, message_type.value, *data.serialize()])

    def __set_socket_options(self):
        self._socket.setsockopt(zmq.IDENTITY, self._identity)

    def __is_valid_message(self, frames: List[bytes]) -> bool:
        if len(frames) < 3:
            logging.error(f"{self.__class__.__name__}: received unexpected frames {frames}")
            return False

        if frames[1] not in {member.value for member in MessageType}:
            logging.error(f"{self._identity}: received unexpected frames {frames}")
            return False

        return True

    def __count_one(self, count_type: Literal["sent", "received"], message_type: MessageType):
        self._statistics[count_type][message_type.name] += 1