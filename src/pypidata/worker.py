from concurrent.futures import ThreadPoolExecutor
from typing import Iterable
from queue import Queue
from threading import Thread

import httpx
from .pypi import get_project
from loguru import logger

class ProducerConsumer:
    def __init__(self, producer, consumer):
        self.queue = Queue()
        self.producer = producer
        self.consumer = consumer

    def consumer_task(self):
        while True:
            data = self.queue.get()
            if data is None:
                break
            self.consumer(data)

    def producer_task(self, input):
        data_count = 0
        for data in self.producer(input):
            if data is not None:
                data_count += 1
                self.queue.put(data)
        return data_count
            
    def start(self, inputs):
        with ThreadPoolExecutor() as executor:
            try:
                # Submit the consumer. We keep the future so
                # that we can check it for errors later
                future = executor.submit(self.consumer_task)
                for _ in executor.map(self.producer_task, inputs):
                    # Check the consumer task each time a producer completes
                    if future.done():
                        future.result()
            except BaseException:
                if future.running():
                    self.queue.put(None)
                # Would be nice to timeout here, that would need t to be a daemon
                raise


def get_project_data(names: Iterable[str]):

    with httpx.Client() as client:
        def p(name):
            yield get_project(name, "simple", client)
            yield get_project(name, "json", client)
        def c(data):
            name, type, data = data
            if data is None:
                logger.error(f"{name}: Data is None")
            else:
                logger.info(f"DATA: {name}, {type}, {len(data)}")
        pc = ProducerConsumer(p, c)
        pc.start(names)

if __name__ == "__main__":
    from pathlib import Path
    names = Path("projects.txt").read_text().splitlines()
    get_project_data(names)