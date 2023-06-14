import json
from collections import ChainMap
from typing import List

import cloudpickle
import copy
import psutil

from scaled.io.async_connector import AsyncConnector
from scaled.protocol.python.message import MessageType, SchedulerStatus
from scaled.scheduler.mixins import Looper, Reporter


class StatusReporter(Looper):
    def __init__(self, binder: AsyncConnector):
        self._managers: List[Reporter] = []
        self._monitor_binder: AsyncConnector = binder

        self._process = psutil.Process()

    def register_managers(self, managers: List[Reporter]):
        self._managers.extend(managers)

    async def routine(self):
        stats = dict(ChainMap(*[await manager.statistics() for manager in self._managers]))

        # Converts function_id to function_name to get number of tasks per function
        function_ids = list(stats["function_manager"]["function_id_to_tasks"].keys())
        for key in function_ids:
            function_name = cloudpickle.loads(stats["function_manager"]["function_id_to_function"][key]).__name__ 
            stats["function_manager"]["function_id_to_tasks"][function_name] = stats["function_manager"]["function_id_to_tasks"][key]
            del stats["function_manager"]["function_id_to_tasks"][key]

        # Associates Task and Function with function names
        for count_type in ["received", "sent"]:
            for key in list(stats["binder"][count_type].keys()):
                split_len = len(key.split('-'))
                if split_len > 1:
                    key_split = key.split("-")
                    message_type = key_split[0]
                    function_id = key_split[1]
                    function_name = cloudpickle.loads(stats["function_manager"]["function_id_to_function"][function_id]).__name__
                    stats["binder"][count_type][f"{message_type}-{function_name}"] = stats["binder"][count_type][key]
                    del stats["binder"][count_type][key]

        del stats["function_manager"]["function_id_to_function"]
        stats["scheduler"] = {"cpu": self._process.cpu_percent() / 100, "rss": self._process.memory_info().rss}
        await self._monitor_binder.send(MessageType.SchedulerStatus, SchedulerStatus(json.dumps(stats).encode()))
