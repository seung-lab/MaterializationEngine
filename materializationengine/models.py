from sqlalchemy import Column, String, Integer, Boolean, DateTime, ForeignKey
from sqlalchemy.orm import relationship
from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()


class AnalysisVersion(Base):
    __tablename__ = "analysisversion"
    id = Column(Integer, primary_key=True)
    dataset = Column(String(100), nullable=False)
    version = Column(Integer, nullable=False)
    time_stamp = Column(DateTime, nullable=False)
    valid = Column(Boolean)

    def __repr__(self):
        return "{}_v{}".format(self.dataset, self.version)


class AnalysisTable(Base):
    __tablename__ = "analysistables"
    id = Column(Integer, primary_key=True)
    aligned_volume = Column(String(100), nullable=False)
    schema = Column(String(100), nullable=False)
    table_id = Column(String(100), nullable=False)
    valid = Column(Boolean)
    created = Column(DateTime, nullable=False)
    analysisversion_id = Column(Integer, ForeignKey("analysisversion.id"))
    analysisversion = relationship("AnalysisVersion")


class AnalysisMetadata(Base):
    __tablename__ = "analysismetadata"
    id = Column(Integer, primary_key=True)
    schema = Column(String(100), nullable=False)
    table_id = Column(String(100), nullable=False)
    valid = Column(Boolean)
    created = Column(DateTime, nullable=False)
    last_updated = Column(DateTime, nullable=True)
