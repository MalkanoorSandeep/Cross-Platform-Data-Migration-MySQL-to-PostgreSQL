from loguru import logger

def record_count(engine, table):
    with engine.connect() as connection:
        query = f"select count(*) from {table}"
        count_obj = connection.execute(query)
        count = count_obj.fetchall()
        return count



