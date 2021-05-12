from sqlalchemy.orm import declarative_base, Session
from sqlalchemy import Column, String, Integer, LargeBinary, DateTime
import zlib

Base = declarative_base()

class JsonData(Base):
    __tablename__ = "json_data"
    name = Column(String, primary_key=True)
    serial = Column(Integer, nullable=False)
    data = Column(LargeBinary)        

class SimpleData(Base):
    __tablename__ = "simple_data"
    name = Column(String, primary_key=True)
    serial = Column(Integer, nullable=False)
    data = Column(LargeBinary)

class Package(Base):
    __tablename__ = "packages"
    name = Column(String, primary_key=True)
    display_name = Column(String)
    last_serial = Column(Integer, nullable=False)

class Changelog(Base):
    __tablename__ = "changelog"
    name = Column(String)
    display_name = Column(String)
    serial = Column(Integer, primary_key=True)
    version = Column(String)
    timestamp = Column(DateTime)
    action = Column(String)

if __name__ == "__main__":
    from sqlalchemy import create_engine
    engine = create_engine('sqlite:///foo.db', echo=True)
    Base.metadata.create_all(engine)
    examples = [
        ("test", 1, b"Hello, world"),
        ("example", 2, b"Another set of data")
    ]
    with Session(engine) as session:
        with session.begin():
            for name, serial, data in examples:
                data = zlib.compress(data)
                obj = JsonData(name=name, serial=serial, data=data)
                session.add(obj)
