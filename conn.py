import pyodbc
import re  # Aunque en este ejemplo no se usa, se deja en caso de que lo necesites para validaciones adicionales

def generate_dbml_from_connection(connection_string, dbml_file="schema.dbml"):
    """
    Genera un archivo DBML a partir de una cadena de conexión a la base de datos.
    """
    cnxn = None  # Inicializamos la conexión para evitar UnboundLocalError
    try:
        cnxn = pyodbc.connect(connection_string)
        cursor = cnxn.cursor()

        # Obtener nombres de las tablas
        cursor.execute("SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_TYPE='BASE TABLE'")
        tables = [row.TABLE_NAME for row in cursor.fetchall()]

        dbml_output = ""

        for table_name in tables:
            dbml_output += f"Table {table_name} {{\n"

            # Obtener información de columnas para cada tabla
            cursor.execute(f"""
                SELECT COLUMN_NAME, DATA_TYPE, IS_NULLABLE, COLUMN_DEFAULT,
                       (CASE WHEN EXISTS (
                           SELECT 1 
                           FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE 
                           WHERE TABLE_NAME = '{table_name}' 
                             AND COLUMN_NAME = a.COLUMN_NAME 
                             AND CONSTRAINT_NAME LIKE 'PK%'
                       ) THEN 1 ELSE 0 END) AS IS_PRIMARY_KEY
                FROM INFORMATION_SCHEMA.COLUMNS a
                WHERE TABLE_NAME = '{table_name}'
                ORDER BY ORDINAL_POSITION
            """)

            for row in cursor.fetchall():
                col_name = row.COLUMN_NAME
                col_type = row.DATA_TYPE
                is_nullable = row.IS_NULLABLE == "YES"
                # column_default se podría usar si se desea mostrar el valor por defecto
                is_primary_key = row.IS_PRIMARY_KEY == 1
                dbml_output += f"  {col_name} {col_type.lower()}"
                if is_primary_key:
                    dbml_output += " [primary key]"
                if is_nullable:
                    dbml_output += " [null]"
                dbml_output += "\n"

            dbml_output += "}\n\n"

        # Extraer relaciones de llaves foráneas
        dbml_output += extract_relationships(cursor, tables)

        with open(dbml_file, "w") as f:
            f.write(dbml_output)

        print(f"Archivo DBML '{dbml_file}' generado correctamente.")

    except pyodbc.Error as ex:
        sqlstate = ex.args[0]
        print(f"Error en la base de datos: {sqlstate}")
        print(ex)
        print("Verifica tu cadena de conexión y que el driver ODBC esté instalado correctamente.")

    except Exception as e:
        print(f"Ocurrió un error: {e}")

    finally:
        if cnxn:
            cnxn.close()

def extract_relationships(cursor, tables):
    """
    Extrae relaciones de llaves foráneas de la base de datos.
    Se utiliza una consulta que une REFERENTIAL_CONSTRAINTS con KEY_COLUMN_USAGE para obtener:
    - La columna de la tabla hija (child)
    - La tabla y columna referenciadas (parent)
    """
    relationship_output = ""
    for table_name in tables:
        cursor.execute(f"""
            SELECT 
                KCU1.COLUMN_NAME AS child_column,
                KCU2.TABLE_NAME AS parent_table,
                KCU2.COLUMN_NAME AS parent_column
            FROM INFORMATION_SCHEMA.REFERENTIAL_CONSTRAINTS RC
            JOIN INFORMATION_SCHEMA.KEY_COLUMN_USAGE KCU1
              ON RC.CONSTRAINT_NAME = KCU1.CONSTRAINT_NAME
            JOIN INFORMATION_SCHEMA.KEY_COLUMN_USAGE KCU2
              ON RC.UNIQUE_CONSTRAINT_NAME = KCU2.CONSTRAINT_NAME
            WHERE KCU1.TABLE_NAME = '{table_name}'
        """)

        for row in cursor.fetchall():
            child_column = row.child_column
            parent_table = row.parent_table
            parent_column = row.parent_column
            relationship_output += f"Ref: {table_name}.{child_column} > {parent_table}.{parent_column}\n"

    return relationship_output

if __name__ == "__main__":
    connection_string = ("Driver={ODBC Driver 18 for SQL Server};"
                         "Server=repladb.database.windows.net,1433;"
                         "Database=pcmi_warranty_custom_PRWS;"
                         "UID=adm0n;"
                         "PWD=TezPCc3bJf2nT9D*;")
    generate_dbml_from_connection(connection_string)
