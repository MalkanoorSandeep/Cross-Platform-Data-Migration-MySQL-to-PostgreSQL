import re


def mysql_postgresql_mapping(mysql_type):
    mapping_values = {
        'varchar' : 'VARCHAR',
        'int' : 'INTEGER',
        'bigint' : 'BIGINT',
        'double' : 'DOUBLE PRECISION',
        'datetime' : 'TIMESTAMP',
        'text' : 'TEXT'
    }

    for mysql, postgres in mapping_values.items():
        if mysql_type.startswith(mysql):
            return re.sub(r'\b' + mysql + r'\b', postgres, mysql_type)
    return mysql_type


