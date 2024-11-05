from db_connection import postgresql_connection
from loguru import logger
from mapping import mysql_postgresql_mapping

def create_table(table_schema):
    target_engine = postgresql_connection()
    with target_engine.connect() as connection:
        for table, column in table_schema.items():
                column_list = []
                for col in column:
                    name = col['name']
                    data_type = col['data_type']
                    constraint = col['constraint']

                    dtype = mysql_postgresql_mapping(data_type)   

                    column_list.append((f'"{name}"', dtype, constraint))
                # formatted_columns = [f"{name} {data_type} {constraint}" for name, data_type, constraint in column_list]
                formatted_columns = ", ".join([f"{name} {col_type} {default}".rstrip(',') for name, col_type, default in column_list])
                logger.info(formatted_columns)
                query = f'create table if not exists {table} ({formatted_columns})'
                connection.execute(query)

    #     for table, column in table_schema.items():
    #         column_list = []
    #         for col in column:
    #              name = col['name']
    #              data_type = col['datatype']
    #              constraint = col['constraint']
    #              column_list.append(name, data_type, constraint)
    #         query = f'create table if not exists {table}()'



        
        # connection.execute()


