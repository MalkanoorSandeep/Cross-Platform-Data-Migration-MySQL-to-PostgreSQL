from db_connection import mysql_db_conection, postgresql_connection
from extract import table_list, table_schema
from loguru import logger
from creating_tables import create_table


if __name__ == '__main__':
    source_engine = mysql_db_conection()
    table_names = table_list(source_engine)
    schema = table_schema(source_engine, table_names)
    create_table(schema)