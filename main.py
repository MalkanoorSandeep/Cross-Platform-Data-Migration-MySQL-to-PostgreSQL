from db_connection import mysql_db_conection, postgresql_connection
from extract import table_list, extract_tables, table_schema
from loguru import logger
from creating_tables import create_table
from count_validation import record_count


if __name__ == '__main__':
    source_engine = mysql_db_conection()
    
    table_names = table_list(source_engine)

    extracted_tables = extract_tables()

    schema, table, column, data = table_schema(source_engine, extracted_tables)
   

    create_table(schema,  data)
 