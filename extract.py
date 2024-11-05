from sqlalchemy import create_engine
from loguru import logger
import pandas as pd
import re


def table_list(engine):
    with engine.connect() as connection:
        query = f'show tables'
        tables = connection.execute(query)
        table_names = ([table[0] for table in tables])
        # logger.info(table_names)
        return table_names
    


def table_schema(engine, table_names):
        with engine.connect() as connection:

             schema_dict ={}
             table_schema = []
             tables =[]
             columns = []


             for table in table_names:
                 query = f'show create table {table}' 
                 schema_obj = connection.execute(query)
                 schema = schema_obj.fetchone()
                
                 table_schema.append(schema)
                 table_name = schema[0]
                 tables.append(schema[0])

                 
                 column_pattern = r"`(\w+)`\s+(\w+(?:\(\d+\))?)\s*(NULL|NOT NULL|DEFAULT\s+\S+|AUTO_INCREMENT)?"
                 column_names = re.findall(column_pattern, schema[1])
                 columns.append(column_names)
                 
                 column = []
                 for col in column_names:
                      column_dict = {
                           'name' : col[0],
                           'data_type' : col[1],
                           'constraint' : col[2] if col[2] else ""
                      }
                      column.append(column_dict)  

                 schema_dict[table_name] = column
                #  logger.info(f"Table name: {table_name}\ncolumns: {column}\n")
             

             logger.info(f"Schema of the table is: {table_schema}")
            #  logger.info(f"The tables in the database are: {tables}")
            #  logger.info(f"The columns are: {columns}")

             return schema_dict



            
                  
    



        







