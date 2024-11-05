import os
from dotenv import load_dotenv
import getpass
from loguru import logger

load_dotenv(f"/Users/sandeepmalkanoor/Documents/Python/Data_Migration/Project1/.env")



def mysql_config():

    MYSQL_CONFIG = {
        'host' : os.getenv('MYSQL_HOST'),
        'user' : os.getenv('MYSQL_USERNAME'),
        'password' : getpass.getpass('Enter the source db "Password" : '),
        'database' : input('Enter the database name: ')
    }

    os.environ['MYSQL_PASSWORD'] = MYSQL_CONFIG['password']
    os.environ['MYSQL_DATABASE'] = MYSQL_CONFIG['database']

    return MYSQL_CONFIG





def postgresql_config():

    POSTGRESQL_CONFIG = {
        'host' : os.getenv('POSTGRESQL_HOST'),
        'user' : os.getenv('POSTGRESQL_USERNAME'),
        'password' : getpass.getpass("Enter the 'password' for target database: "),
        'database' : input("Enter the database name for migrating tables: "),
        'port' : os.getenv('POSTGRESQL_PORT')
    }

    os.environ['POSTGRESQL_PASSWORD'] = POSTGRESQL_CONFIG['password']
    os.environ['POSTGRESQL_DATABASE'] = POSTGRESQL_CONFIG['database']

    return POSTGRESQL_CONFIG
