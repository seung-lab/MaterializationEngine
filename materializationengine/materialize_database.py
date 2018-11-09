from sqlalchemy import create_engine
from sqlalchemy.orm import scoped_session, sessionmaker
from emannotationschemas.models import Base
engine = create_engine('postgresql://postgres:welcometothematrix@35.196.105.34/postgres', convert_unicode=True)
db_session = scoped_session(sessionmaker(autocommit=False,
                                         autoflush=False,
                                         bind=engine))
Base.query = db_session.query_property()
