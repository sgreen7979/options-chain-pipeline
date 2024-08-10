#!/usr/bin/env python3
import datetime as dt
import os
import textwrap
from typing import Dict
from typing import List
import xml.etree.ElementTree as ET

from daily.env import DATA_PATH
from daily.sql.mssql.client import MSSQLClient
from daily.sql.mssql.config import MSSQLConfig


SQL_DATA_PATH = f"{DATA_PATH}/sql"
SCHEMA_COL_MAP = {
    "COLUMN_NAME": {"key": "name", "ord": 0},
    "DATA_TYPE": {"key": "type", "ord": 1},
    "IS_NULLABLE": {"key": "nullable", "ord": 2},
    "COLUMN_DEFAULT": {"key": "default", "ord": 3},
    "NUMERIC_PRECISION": {"key": "numeric_precision", "ord": 4},
    "NUMERIC_PRECISION_RADIX": {"key": "numeric_precision_radix", "ord": 5},
    "NUMERIC_SCALE": {"key": "numeric_scale", "ord": 6},
    "DATETIME_PRECISION": {"key": "datetime_precision", "ord": 7},
    "COLLATION_NAME": {"key": "collation_name", "ord": 8},
    "ORDINAL_POSITION": {"key": "ordinal_position", "ord": 9},
    "CHARACTER_MAXIMUM_LENGTH": {"key": "character_maximum_length", "ord": 10},
    "CHARACTER_OCTET_LENGTH": {"key": "character_octet_length", "ord": 11},
    "CHARACTER_SET_NAME": {"key": "character_set_name", "ord": 12},
}
SCHEMA_QUERY_COLS = list(SCHEMA_COL_MAP.keys())
SCHEMA_QUERY_COLS_STR = "\n\t, ".join(SCHEMA_QUERY_COLS)
TABLES_QUERY = (
    "SELECT TABLE_NAME "
    "FROM INFORMATION_SCHEMA.TABLES "
    "WHERE TABLE_TYPE='BASE TABLE';"
)


def get_schema_query(table_name: str) -> str:
    # return f"""
    # SELECT
    #     COLUMN_NAME
    #     , DATA_TYPE
    #     , IS_NULLABLE
    #     , COLUMN_DEFAULT
    #     , NUMERIC_PRECISION
    #     , NUMERIC_PRECISION_RADIX
    #     , NUMERIC_SCALE
    #     , DATETIME_PRECISION
    #     , COLLATION_NAME
    #     , ORDINAL_POSITION
    #     , CHARACTER_MAXIMUM_LENGTH
    #     , CHARACTER_OCTET_LENGTH
    #     , CHARACTER_SET_NAME
    # FROM INFORMATION_SCHEMA.COLUMNS
    # WHERE TABLE_NAME = '{table_name}';
    # """
    return textwrap.dedent(
        f"""
        SELECT
            {SCHEMA_QUERY_COLS_STR}
        FROM INFORMATION_SCHEMA.COLUMNS
        WHERE TABLE_NAME = '{table_name}';
        """
    )


def get_xml_str() -> str:
    # Initialize sql client
    sql_client = MSSQLClient(MSSQLConfig.ConfiguredConnectionString, autocommit=True)

    # Initialize root XML element
    root = ET.Element("database")

    with sql_client as sql_client:
        # Query for tables
        tables = sql_client.execute(
            "SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_TYPE='BASE TABLE';"
        ).fetchall()
        for table_name in tables:
            # Get table_name str
            table_name = table_name[0]
            # Add a XML table element to root
            table_element = ET.SubElement(root, 'table', name=table_name)
            # Query for column data of table
            columns = sql_client.execute(
                f"""
                SELECT
                    COLUMN_NAME
                    , DATA_TYPE
                    , IS_NULLABLE
                    , COLUMN_DEFAULT
                    , NUMERIC_PRECISION
                    , NUMERIC_PRECISION_RADIX
                    , NUMERIC_SCALE
                    , DATETIME_PRECISION
                    , COLLATION_NAME
                    , ORDINAL_POSITION
                    , CHARACTER_MAXIMUM_LENGTH
                    , CHARACTER_OCTET_LENGTH
                    , CHARACTER_SET_NAME
                FROM INFORMATION_SCHEMA.COLUMNS
                WHERE TABLE_NAME = '{table_name}';
                """
            ).fetchall()

            for col in columns:
                # Add column element to table element of root
                col_element = ET.SubElement(table_element, 'column')
                # Populate column element data
                ET.SubElement(col_element, 'name').text = col[0]
                ET.SubElement(col_element, 'type').text = col[1]
                ET.SubElement(col_element, 'nullable').text = col[2]
                ET.SubElement(col_element, 'default').text = str(
                    col[3] if col[3] is not None else 'None'
                )
                ET.SubElement(col_element, 'numeric_precision').text = str(
                    col[4] if col[4] is not None else 'None'
                )
                ET.SubElement(col_element, 'numeric_precision_radix').text = str(
                    col[5] if col[5] is not None else 'None'
                )
                ET.SubElement(col_element, 'numeric_scale').text = str(
                    col[6] if col[6] is not None else 'None'
                )
                ET.SubElement(col_element, 'datetime_precision').text = str(
                    col[7] if col[7] is not None else 'None'
                )
                ET.SubElement(col_element, 'collation_name').text = str(
                    col[8] if col[8] is not None else 'None'
                )
                ET.SubElement(col_element, 'ordinal_position').text = str(
                    col[9] if col[9] is not None else 'None'
                )
                ET.SubElement(col_element, 'character_maximum_length').text = str(
                    col[10] if col[10] is not None else 'None'
                )
                ET.SubElement(col_element, 'character_octet_length').text = str(
                    col[11] if col[11] is not None else 'None'
                )
                ET.SubElement(col_element, 'character_set_name').text = str(
                    col[12] if col[12] is not None else 'None'
                )
    return ET.tostring(root, encoding='unicode')


def get_xml_str_NEW() -> str:
    # Initialize sql client
    sql_client = MSSQLClient(MSSQLConfig.ConfiguredConnectionString, autocommit=True)

    # Initialize root XML element
    root = ET.Element("database")

    with sql_client as sql_client:
        # Query for tables
        tables = sql_client.execute(TABLES_QUERY).fetchall()
        for table_name in tables:
            # Get table_name str
            table_name = table_name[0]
            # Add a XML table element to root
            table_element = ET.SubElement(root, 'table', name=table_name)
            # Query for column data of table
            columns = sql_client.execute(get_schema_query(table_name)).fetchall()
            for col in columns:
                # Add column element to table element of root
                col_element = ET.SubElement(table_element, 'column')
                # Populate XML column data
                for idx, field in enumerate(SCHEMA_QUERY_COLS):
                    key = SCHEMA_COL_MAP[field]["key"]
                    ET.SubElement(col_element, key).text = str(
                        col[idx] if col[idx] is not None else 'None'
                    )
    return ET.tostring(root, encoding='unicode')


def to_disk(xml_str: str, out_file: str) -> str:
    # directory = "C:\\Users\\green\\daily\\daily\\DATA\\sql"
    # file_path = f"{directory}\\xml_schema_stock_database.xml"
    today = dt.date.today().isoformat()
    directory = f"{SQL_DATA_PATH}/schema/{today}"
    if not os.path.exists(directory):
        os.makedirs(directory, exist_ok=True)
    file_path = f"{directory}/{out_file}"
    with open(file_path, "w") as f:
        f.write(xml_str)
    return file_path


def get_xml_tree(file_path: str) -> ET.ElementTree:
    with open(file_path, "r") as f:
        tree = ET.parse(f)
    return tree


def create_db_structure_dict(file_path: str) -> Dict:
    tree = get_xml_tree(file_path)
    database_structure = {}

    # Iterate through each table in the database
    for table in tree.getroot().findall('table'):
        table_name = table.get('name')
        database_structure[table_name] = []

        # Iterate through each column in the table
        for column in table.findall('column'):
            column_details = {
                'name': column.find('name').text,  # type: ignore
                'type': column.find('type').text,  # type: ignore
                'nullable': column.find('nullable').text,  # type: ignore
                'default': column.find('default').text,  # type: ignore
                'numeric_precision': column.find('numeric_precision').text,  # type: ignore
                'numeric_precision_radix': column.find('numeric_precision_radix').text,  # type: ignore
                'numeric_scale': column.find('numeric_scale').text,  # type: ignore
                'datetime_precision': column.find('datetime_precision').text,  # type: ignore
                'collation_name': column.find('collation_name').text,  # type: ignore
                'ordinal_position': column.find('ordinal_position').text,  # type: ignore
                'character_maximum_length': column.find('character_maximum_length').text,  # type: ignore
                'character_octet_length': column.find('character_octet_length').text,  # type: ignore
                'character_set_name': column.find('character_octet_length').text,  # type: ignore
            }
            database_structure[table_name].append(column_details)
    return database_structure


def generate_sql_create_statements(database_structure: Dict) -> List[str]:
    sql_statements = []

    for table_name, columns in database_structure.items():
        # Start the CREATE TABLE statement
        create_statement = f"CREATE TABLE {table_name} (\n"

        # List to hold column definitions
        column_definitions = []
        for column in columns:
            # Prepare the column definition
            column_name = column['name']
            column_type = column['type']
            nullable = "NOT NULL" if column['nullable'] == "NO" else "NULL"
            default = (
                f" DEFAULT {column['default']}" if column['default'] != "None" else ""
            )

            # Handle specific data type mappings or adjustments if necessary
            if column_type == "decimal":
                precision = column["numeric_precision"]
                scale = column["numeric_scale"]
                column_type = (
                    f"{column_type}({precision},{scale})"  # Specify precision and scale
                )
            elif column_type == "nvarchar":
                max_len = column["character_maximum_length"]
                column_type = (
                    f"{column_type}({max_len})"  # Specify max_character_length
                )

            # Combine into a single column definition string
            column_definition = f"  {column_name} {column_type} {nullable}{default}"
            column_definitions.append(column_definition)

        # Combine all column definitions and close the CREATE TABLE statement
        create_statement += ",\n".join(column_definitions)
        create_statement += "\n);"
        sql_statements.append(create_statement)

    return sql_statements


def main():
    xml_str = get_xml_str()
    file_path = to_disk(xml_str, "xml_schema_stock_database.xml")
    db_structure = create_db_structure_dict(file_path)
    sql_statements = generate_sql_create_statements(db_structure)
    print("\n\n".join(sql_statements))


if __name__ == "__main__":
    main()
