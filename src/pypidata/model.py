from sqlalchemy.orm import declarative_base, Session
from sqlalchemy import MetaData, Table, Column, String, Integer, LargeBinary, DateTime
import zlib

metadata = MetaData()

pypi_data = Table("pypi_data", metadata,
    Column("name", String, primary_key=True),
    Column("serial", Integer, nullable=False),
    Column("json_data", LargeBinary),
    Column("simple_data", LargeBinary),
)

packages = Table("packages", metadata,
    Column("name", String, primary_key=True),
    Column("display_name", String),
    Column("last_serial", Integer, nullable=False),
)

changelog = Table("changelog", metadata,
    Column("name", String),
    Column("display_name", String),
    Column("serial", Integer, primary_key=True),
    Column("version", String),
    Column("timestamp", DateTime),
    Column("action", String),
)

if __name__ == "__main__":
    from sqlalchemy import create_engine
    engine = create_engine('sqlite:///foo.db', echo=True)
    metadata.create_all(engine)
    examples = [
        ("test", 1, b"Hello, world"),
        ("example", 2, b"Another set of data")
    ]
    with engine.begin() as conn:
        conn.execute(
            pypi_data.insert(),
            [
                dict(name=name, serial=serial, json_data=zlib.compress(data))
                for name, serial, data in examples
            ]
        )
