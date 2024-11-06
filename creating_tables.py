from db_connection import postgresql_connection
from loguru import logger
from mapping import mysql_postgresql_mapping
from count_validation import record_count
from fetch_load import load_record


# Function which will create tables in the target database.

def create_table(table_schema, data):

    target_engine = postgresql_connection()
    with target_engine.connect() as connection:


        # Getting the table names at run time to migrate from source to target database.

        table_names_input = input("Please provide the table names(comma-separated) to migrate from mysql to postgres: ")
        table_name = {name.strip() for name in table_names_input.split(",")}

        migrated_tables = set()


        counter = 0
        # Building the logic using the for loop to get exact create table statement.

        for table, column in table_schema.items():
                
                
                column_list = []
                col_names = []
                for col in column:
                    name = col['name']
                    data_type = col['data_type']
                    constraint = col['constraint']

                    dtype = mysql_postgresql_mapping(data_type)  

                    col_names.append(f'{name}') 

                    column_list.append((f'"{name}"', dtype, constraint))

                formatted_columns = ", ".join([f"{name} {col_type} {default}".rstrip(',') for name, col_type, default in column_list])
                

                # Count validation after creating the tables in target
                if table in table_name:
                    query = f'create table if not exists {table} ({formatted_columns})'
                    connection.execute(query)
                    migrated_tables.add(table)
                    table_count = record_count(target_engine, table)
                    logger.info(f"After Migration the {table} contains: {table_count[0]} records")
                
                    
                    logger.info(f"the data counter value is {data[counter]}")
                    load_record(target_engine, table, col_names, data[counter])

                    count_after_migration = record_count(target_engine, table)
                    logger.info(f"The count after the migration is: {count_after_migration}")

                counter = counter+1
                

        not_migrated_tables = table_name - migrated_tables

        logger.info(f"Migrated tables are: {migrated_tables}")
        # logger.info(f"Tables which are not migrated: {not_migrated_tables}")
        
        # logger.info(f"These tables {not_migrated_tables} are not present in the source database please enter valid table names")



