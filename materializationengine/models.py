from sqlalchemy import Column, Integer, String, DateTime, ForeignKey
from sqlalchemy.orm import relationship
from materializationengine.database import Base


class AnalysisVersion(Base):
    __tablename__ = 'analysisversion'
    id = Column(Integer, primary_key=True)
    dataset = Column(String(100), nullable=False)
    version = Column(Integer, nullable=False)
    time_stamp = Column(DateTime, nullable=False)

    def __repr__(self):
        return "{}_v{}".format(self.dataset, self.version)


class AnalysisTable(Base):
    __tablename__ = 'analysistables'
    id = Column(Integer, primary_key=True)
    schema = Column(String(100), nullable=False)
    tablename = Column(String(100), nullable=False)
    analysisversion_id = Column(Integer, ForeignKey('analysisversion.id'))
    analysisversion = relationship('AnalysisVersion')

