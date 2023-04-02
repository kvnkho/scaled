import random

from scaled.client import Client
from scaled.cluster.combo import SchedulerClusterCombo
from scaled.cluster.scheduler import SchedulerProcess
from scaled.utility.zmq_config import ZMQConfig

from scaled.io.config import (
    DEFAULT_HEARTBEAT_INTERVAL_SECONDS,
    DEFAULT_FUNCTION_RETENTION_SECONDS,
    DEFAULT_IO_THREADS,
    DEFAULT_LOAD_BALANCE_SECONDS,
    DEFAULT_LOAD_BALANCE_TRIGGER_TIMES,
    DEFAULT_MAX_NUMBER_OF_TASKS_WAITING,
    DEFAULT_WORKER_PROCESSING_QUEUE_SIZE,
    DEFAULT_WORKER_TIMEOUT_SECONDS,
    DEFAULT_GARBAGE_COLLECT_INTERVAL_SECONDS,
    DEFAULT_TRIM_MEMORY_THRESHOLD_BYTES,
    DEFAULT_PER_WORKER_QUEUE_SIZE,
)

from scaled.utility.logging.scoped_logger import ScopedLogger
from scaled.utility.logging.utility import setup_logger 

import logging

def sleep_print(sec: int):
    setup_logger()
    logging.info("test")
    return sec * 1

def main():
    setup_logger()

    address = "tcp://127.0.0.1:2345"
    network_log_subscriber_address = "tcp://127.0.0.1:3456"
    network_log_publisher_address = "tcp://127.0.0.1:3457"

    # scheduler = SchedulerProcess(address=ZMQConfig.from_string(address),
    #     io_threads=DEFAULT_IO_THREADS,
    #     max_number_of_tasks_waiting=DEFAULT_MAX_NUMBER_OF_TASKS_WAITING,
    #     per_worker_queue_size=DEFAULT_PER_WORKER_QUEUE_SIZE,
    #     worker_timeout_seconds=DEFAULT_WORKER_TIMEOUT_SECONDS,
    #     function_retention_seconds=DEFAULT_FUNCTION_RETENTION_SECONDS,
    #     load_balance_seconds=DEFAULT_LOAD_BALANCE_SECONDS,
    #     load_balance_trigger_times=DEFAULT_LOAD_BALANCE_TRIGGER_TIMES,

    # ).start()

    cluster = SchedulerClusterCombo(address=address, 
    n_workers=2, per_worker_queue_size=2, event_loop="builtin",
        network_log_subscriber_address=network_log_subscriber_address,
        network_log_publisher_address=network_log_publisher_address
    )
    # client = Client(address=address)

    # tasks = [random.randint(0, 101) for _ in range(10)]

    # with ScopedLogger(f"scaled submit {len(tasks)} tasks"):
    #     futures = [client.submit(sleep_print, a) for a in tasks]

    # with ScopedLogger(f"scaled gather {len(futures)} results"):
    #     results = [future.result() for future in futures]

if __name__ == "__main__":
    main()