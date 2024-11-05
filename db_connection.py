from sqlalchemy import create_engine
from config import mysql_config, postgresql_config
from loguru import logger
import urllib.parse



def mysql_db_conection():

    SOURCE_DB = mysql_config()

    username = SOURCE_DB['user']
    password = urllib.parse.quote_plus(SOURCE_DB['password'])
    host = SOURCE_DB['host']
    database= SOURCE_DB['database']

    try:
        engine = create_engine(
            f"mysql+pymysql://{username}:{password}@{host}/{database}"
        )

        with engine.connect() as connection:
            logger.info(f"Successfully connected to the source database {database}")
        return engine
    
    except Exception as e:
        logger.info(f"Source database connection error: {e}")
        return None
    



def postgresql_connection():

    TARGET_DB = postgresql_config()

    host = TARGET_DB['host']
    username = TARGET_DB['user']
    password = urllib.parse.quote_plus(TARGET_DB['password'])
    database = TARGET_DB['database']
    port = TARGET_DB['port']

    try:
        engine = create_engine(
            f'postgresql+pg8000://{username}:{password}@{host}:{port}/{database}'
        )

        with engine.connect() as connected:
            logger.info(f"Successfully connected to the target database {database}")
        return engine
    
    except Exception as e:
        logger.info(f"Target databse connection error: {e}")

    