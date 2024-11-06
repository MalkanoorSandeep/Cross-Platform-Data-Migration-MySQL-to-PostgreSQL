from sqlalchemy import create_engine
from count_validation import record_count
from fetch_load import fetch_records
from loguru import logger
import pandas as pd
import re


def table_list(engine):
    with engine.connect() as connection:
        query = f'show tables'
        tables = connection.execute(query)
        table_names = ([table[0] for table in tables])
        logger.info(f"No of tables in the source database: {table_names}")
        return table_names
    

def extract_tables():
     table_to_extract = input("Please provide the table names(comma-separated) to Extract data from mysql: ")
     extracted_tables = {name.strip() for name in table_to_extract.split(",")}
     logger.info(f"The extracted tables are: {extracted_tables}")
     return extracted_tables


def table_schema(engine, table_names):
        with engine.connect() as connection:

             schema_dict ={}
             table_schema = []
             tables =[]
             columns = []
             total_data =[]



            # Loop over rhe table to fetch data, to count records, to get schema.

             for table in table_names:
                 

                # Counting the number of records in ex=ach table by calling the record_count function
                 table_count = record_count(engine, table)
                 logger.info(f"The table '{table}' in source database contains: {table_count[0]} records")



                # Fetching the data from the tables by calling fetch_records function and storing in variable total_data 
                 data = fetch_records(engine, table)
                 total_data.append(data)



                # Logic to fetch the schema of the table by using the sql query "SHOW CREATE table_name"
                 query = f'show create table {table}' 
                 schema_obj = connection.execute(query)
                 schema = schema_obj.fetchone()
                 table_schema.append(schema)            # Storing the schema in table_schema variale

                 # Schema contains two values, "table name" and "columns with datatypes"
                
                 table_name = schema[0]            # Schema[0] contains table names so storing in the variable table_name
                 tables.append(schema[0])

                
                # Creating the regular expression to extract the required pattern froom the given schema.

                # # Example:
                # '''CREATE TABLE `ganait` (\n  `employee` varchar(10) DEFAULT NULL,\n  `recruiter` varchar(10) DEFAULT NULL,\n
                # In the above give create table statement the regular expression will retrieve only the "employee varchar(10) DEFAULT NULL" ''' 
                # we used r"`(\w+)`\s+(\w+(?:\(\d+\))?)\s*(NULL|NOT NULL|DEFAULT\s+\S+|AUTO_INCREMENT)?" as reg ex.
            
                 column_pattern = r"`(\w+)`\s+(\w+(?:\(\d+\))?)\s*(NULL|NOT NULL|DEFAULT\s+\S+|AUTO_INCREMENT)?"
                 column_names = re.findall(column_pattern, schema[1])
                 columns.append(column_names)
                 

                 # Creating columns as a dictionary elements.
                 column = []
                 for col in column_names:
                      column_dict = {
                           'name' : col[0],
                           'data_type' : col[1],
                           'constraint' : col[2] if col[2] else ""
                      }
                      column.append(column_dict)  

                 schema_dict[table_name] = column

                 
             
             logger.info(f"There are {len(tables)} tables in source database which are: {tables}")
            
             return schema_dict, tables, columns, total_data



            
                  
    



        







