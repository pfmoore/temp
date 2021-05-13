import sys
import httpx
import zlib

from .model import PyPIDB
from .worker import ProducerConsumer
from .pypi import get_project

class Processor:
    def __init__(self, client, engine):
        self.client = client
        self.engine = engine
        self.db = PyPIDB()
        self.db.create_all(engine)

    def __enter__(self):
        # TODO: This isn't right...
        self.conn = self.engine.begin().__enter__()
        return self

    def __exit__(self, *exc):
        conn = self.conn
        self.conn = None
        conn.__exit__(*exc)

    def producer(self, name):
        serial, simple_data, json_data = get_project(name, self.client)
        yield name, serial, zlib.compress(simple_data), zlib.compress(json_data)

    def consumer(self, data):
        self.conn.execute(
            self.db.pypi_data.insert(),
            [
                dict(name=name, serial=serial, simple_data=simple_data, json_data=json_data)
                for name, serial, simple_data, json_data in data
            ]
        )



def main():
    names = sys.argv[1:]
    pc = ProducerConsumer()
    print("Main called")