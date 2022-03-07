from ..config import settings
from sqlalchemy import create_engine
from sqlalchemy import MetaData


class DBSession:
    def __init__(self):
        self.engine = self.create_engine()

        self.meta = MetaData()
        self.meta.reflect(bind=self.engine)

    @staticmethod
    def create_engine():
        return create_engine(
            f'mysql+mysqlconnector://'
            f'{settings.dbms_secrets.login}'
            f':'
            f'{settings.dbms_secrets.password}'
            f'@'
            f'{settings.dbms_host}'
            f'/'
            f'{settings.database}',
            echo=False
        )
