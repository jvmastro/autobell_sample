import os
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker


class HerokuDBConnector:
    
    def __init__(self, **kwargs):
        self.DB_URI = os.environ['DATABASE_URL'].replace('postgres','postgresql').strip()

        # Now create the engine
        self.engine = create_engine(self.DB_URI, echo=True)
        # Make the session maker
        self.session_maker = sessionmaker(bind=self.engine)

    @property
    def session(self):
        """Return a session as a property"""
        return self.session_maker()