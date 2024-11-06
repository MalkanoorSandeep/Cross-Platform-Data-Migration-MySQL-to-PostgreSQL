from loguru import logger


def fetch_records(engine, table):
    with engine.connect() as connection:
        query = f'select * from {table}'
        data_obj = connection.execute(query)
        data = data_obj.fetchall()
        # for value in data:
        return data



def load_record(engine, table, columns, values):
    with engine.connect() as connection:
        
        for value in values:
            placeholders = ", ".join(["%s"] * len(value))
            column = ", ".join([f'"{col}"' for col in columns])
            query = f"INSERT INTO {table} ({column}) VALUES ({placeholders})"
            logger.info(f"{value}")
            connection.execute(query, value)
        # logger.info(f"Record inserted into {table}.")
