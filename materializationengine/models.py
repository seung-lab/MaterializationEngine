from sqlalchemy import Column, String, Integer, Boolean, \
                       DateTime, ForeignKey
from sqlalchemy.orm import relationship
from sqlalchemy.ext.declarative import declarative_base
Base = declarative_base()


class AnalysisVersion(Base):
    __tablename__ = 'analysisversion'
    id = Column(Integer, primary_key=True)
    dataset = Column(String(100), nullable=False)
    version = Column(Integer, nullable=False)
    time_stamp = Column(DateTime, nullable=False)
    valid = Column(Boolean)

    def __repr__(self):
        return "{}_v{}".format(self.dataset, self.version)


class AnalysisTable(Base):
    __tablename__ = 'analysistables'
    id = Column(Integer, primary_key=True)
    schema = Column(String(100), nullable=False)
    tablename = Column(String(100), nullable=False)
    valid = Column(Boolean)
    analysisversion_id = Column(Integer, ForeignKey('analysisversion.id'))
    analysisversion = relationship('AnalysisVersion')
