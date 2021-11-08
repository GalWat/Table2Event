from ..config import settings
from sqlalchemy import create_engine


class DBSession:
    def __init__(self):
        pass

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
            echo=True
        )
