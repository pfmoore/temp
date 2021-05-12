from concurrent.futures import ThreadPoolExecutor
from typing import Iterable, List, Tuple
import queue
from threading import Thread
import json
import zlib

import httpx
from .pypi import get_project
from loguru import logger

class ProducerConsumer:
    def __init__(self, producer, consumer):
        self.queue = queue.Queue()
        self.producer = producer
        self.consumer = consumer

    def consumer_task(self):
        while True:
            data = self.queue.get()
            if data is None:
                break
            # Batch up all pending data
            data = [data]
            while True:
                try:
                    data.append(self.queue.get_nowait())
                except queue.Empty:
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


def producer(name: str):
    simple_data = get_project(name, "simple", client)
    json_data = get_project(name, "json", client)
    serial = json.loads(json_data).get("last_serial")
    yield name, serial, json_data, simple_data

def consumer(items: List[Tuple[str, int, bytes, bytes]]):
    conn.execute(
        pypi_data.insert(),
        [
            {
                "name": name,
                "serial": serial,
                "json_data": zlib.compress(json_data),
                "simple_data": zlib.compress(simple_data),
            }
            for name, serial, json_data, simple_data in items
        ]

    )

def get_project_data(names: Iterable[str]):

    with httpx.Client() as client:
        def p(name):
            yield get_project(name, "simple", client)
            yield get_project(name, "json", client)
        def c(items):
            for name, type, data in items:
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