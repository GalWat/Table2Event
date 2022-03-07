from sqlalchemy import MetaData, create_engine
from sqlalchemy.orm import Session
from ..config import settings


class DBConnection:
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

    def execute(self, *args, **kwargs):
        with Session(self.engine) as session:
            return session.execute(*args, **kwargs)

