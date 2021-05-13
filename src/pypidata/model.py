from sqlalchemy import MetaData, Table, Column, String, Integer, LargeBinary, DateTime


class PyPIDB:
    def __init__(self):
        self.metadata = MetaData()

        self.pypi_data = Table("pypi_data", self.metadata,
            Column("name", String, primary_key=True),
            Column("serial", Integer, nullable=False),
            Column("json_data", LargeBinary),
            Column("simple_data", LargeBinary),
        )

        self.packages = Table("packages", self.metadata,
            Column("name", String, primary_key=True),
            Column("display_name", String),
            Column("last_serial", Integer, nullable=False),
        )

        self.changelog = Table("changelog", self.metadata,
            Column("name", String),
            Column("display_name", String),
            Column("serial", Integer, primary_key=True),
            Column("version", String),
            Column("timestamp", DateTime),
            Column("action", String),
        )

    def create_all(self, engine):
        self.metadata.create_all(engine)

if __name__ == "__main__":
    from sqlalchemy import create_engine
    import zlib
    engine = create_engine('sqlite:///foo.db', echo=True)
    db = PyPIDB()
    db.create_all(engine)
    examples = [
        ("test", 1, b"Hello, world"),
        ("example", 2, b"Another set of data")
    ]
    with engine.begin() as conn:
        conn.execute(
            db.pypi_data.insert(),
            [
                dict(name=name, serial=serial, json_data=zlib.compress(data))
                for name, serial, data in examples
            ]
        )
