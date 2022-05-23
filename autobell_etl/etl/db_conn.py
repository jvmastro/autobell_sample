import subprocess
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker


class HerokuDBConnection:
    
    def __init__(self, **kwargs):
        self.DB_URI = subprocess.run(["heroku", "config:get","DATABASE_URL", "--app", "autobell-beta"], stdout=subprocess.PIPE).stdout.decode('utf-8').replace('postgres','postgresql').strip()


        # Now create the engine
        self.engine = create_engine(self.DB_URI, echo=True)
        # Make the session maker
        self.session_maker = sessionmaker(bind=self.engine)

    @property
    def session(self):
        """Return a session as a property"""
        return self.session_maker()
