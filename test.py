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
from scaled.utility.logging.network import NetworkLogPublisher

import logging

def sleep_print(sec: int):
    setup_logger()
    logging.warning("testing inside function")
    return sec * 1

def main():
    setup_logger()

    address = "tcp://127.0.0.1:2345"
    network_log_forwarding_address = "tcp://127.0.0.1:3456"
    network_log_publisher_address = "tcp://127.0.0.1:3457"

    cluster = SchedulerClusterCombo(address=address, 
        n_workers=2, per_worker_queue_size=2, event_loop="builtin",
        network_log_forwarding_address=network_log_forwarding_address,
        network_log_level=logging.ERROR
    )

    client = Client(address=address, log_address=network_log_publisher_address)

    tasks = [random.randint(0, 101) for _ in range(10)]

    with ScopedLogger(f"scaled submit {len(tasks)} tasks"):
        futures = [client.submit(sleep_print, a) for a in tasks]

    with ScopedLogger(f"scaled gather {len(futures)} results"):
        results = [future.result() for future in futures]

if __name__ == "__main__":
    main()