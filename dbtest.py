import os
import random
import string
from datetime import datetime

import sqlite3
from sqlalchemy import *

from loguru import logger

def randomdata(n):
    for id in range(n):
        numval = random.randint(1,1000)
        textval = ''.join(random.choices(string.ascii_letters, k=50))
        yield id, numval, textval


if __name__ == "__main__":
    from pathlib import Path
    logger.debug("Delete old DB")
    Path("test.db").unlink(missing_ok=True)

    logger.debug("Create engine")
    engine = create_engine("sqlite:///test.db")
    metadata = MetaData()
    test = Table('test', metadata,
        Column('id', Integer, primary_key=True),
        Column('numval', Integer),
        Column('textval', String),
    )
    logger.debug("Create tables")
    metadata.create_all(engine)
    stmt = insert(test)
    logger.debug("Connect")
    with engine.begin() as connection:
        logger.debug("Build data")
        data = [dict(id=id, numval=numval, textval=textval) for id, numval, textval in randomdata(1_000_000)]
        logger.debug("Start insert")
        start = datetime.now()
        connection.execute(stmt, data)
    logger.debug("Time taken: {dur}", dur=datetime.now() - start)
    #with sqlite3.connect("test.db") as conn:
    #    conn.execute("BEGIN")
    #    start = datetime.now()
    #    conn.executemany("INSERT INTO test VALUES (?,?,?)", randomdata(1_000_000))
    #print("Time taken:", datetime.now() - start)
