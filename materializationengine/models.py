from sqlalchemy import Column, Integer, String, DateTime, ForeignKey
from sqlalchemy.orm import relationship
from emannotationschemas.models import TSBase


class AnalysisVersion(TSBase):
    __tablename__ = 'analysisversion'
    dataset = Column(String(100), nullable=False)
    version = Column(Integer, nullable=False)
    time_stamp = Column(DateTime, nullable=False)

    def __repr__(self):
        return "{}_v{}".format(self.dataset, self.version)


class AnalysisTable(TSBase):
    __tablename__ = 'analysistables'
    schema = Column(String(100), nullable=False)
    tablename = Column(String(100), nullable=False)
    analysisversion_id = Column(Integer, ForeignKey('analysisversion.id'))
    analysisversion = relationship('AnalysisVersion')

