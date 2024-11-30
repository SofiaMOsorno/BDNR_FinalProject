#!/usr/bin/env python3
import logging
from datetime import datetime, timedelta
import uuid
import csv

# Set logger
log = logging.getLogger()

CREATE_KEYSPACE = """
        CREATE KEYSPACE IF NOT EXISTS {}
        WITH replication = {{ 'class': 'SimpleStrategy', 'replication_factor': {} }}
"""

CREATE_RESTAURANT_SALES_TABLE = """
    CREATE TABLE IF NOT EXISTS sales_by_restaurant (
        restaurant TEXT,
        month TEXT,
        total_sales DECIMAL,
        PRIMARY KEY ((restaurant), month)
    )
"""

CREATE_MONTHLY_SALES_TABLE = """
    CREATE TABLE IF NOT EXISTS sales_by_month (
        month TEXT,
        restaurant TEXT,
        total_sales DECIMAL,
        PRIMARY KEY ((month), restaurant)
    )WITH CLUSTERING ORDER BY (restaurant ASC)
"""

CREATE_RESTAURANT_FILTERED_SALES_TABLE = """
    CREATE TABLE IF NOT EXISTS sales_by_total (
        month TEXT,
        total_sales DECIMAL,
        restaurant TEXT,
        PRIMARY KEY ((month), total_sales, restaurant)
    ) WITH CLUSTERING ORDER BY (total_sales DESC, restaurant ASC)
"""

# Queries

SELECT_CURRENT_MONTH_SALES = """
    SELECT *
    FROM sales_by_month
    WHERE month = ?
"""

SELECT_CURRENT_MONTH_SALES_TOP = """
    SELECT *
    FROM sales_by_total
    WHERE month = ?
    LIMIT 3
"""

SELECT_ALL_MONTHLY_SALES = """ 
    SELECT * 
    FROM sales_by_month 
"""

SELECT_MONTHLY_SALES = """ 
    SELECT restaurant, total_sales
    FROM sales_by_month 
    WHERE month = ? 
"""

SELECT_RESTAURANT_SALES = """ 
    SELECT month, total_sales
    FROM sales_by_restaurant
    WHERE restaurant = ? 
"""

SELECT_MONTHLY_RESTAURANT_SALES = """ 
    SELECT *
    FROM sales_by_restaurant
    WHERE restaurant = ? 
    AND month = ? 
"""

SELECT_SALES_IN_RANGE = """
    SELECT * 
    FROM sales_by_total 
    WHERE month = ? 
    AND total_sales >= ? 
    AND total_sales <= ?
"""

def create_keyspace(session, keyspace, replication_factor):
    log.info(f"Creando espacio de claves: {keyspace} con factor de replicación {replication_factor}")
    session.execute(CREATE_KEYSPACE.format(keyspace, replication_factor))

def create_schema(session):
    log.info("Creando el esquema del modelo")
    session.execute(CREATE_RESTAURANT_SALES_TABLE)
    session.execute(CREATE_MONTHLY_SALES_TABLE)
    session.execute(CREATE_RESTAURANT_FILTERED_SALES_TABLE)

def uuid_from_time(date):
    timestamp = date.timestamp()
    return uuid.uuid1(node=int(timestamp))

def convert_uuid(date):
    return uuid_from_time(date)

# Función 1
def get_current_month_sales(session):
    current_month = datetime.now().strftime('%B')
    log.info(f"Recuperando totales de {current_month}")
    stmt = session.prepare(SELECT_CURRENT_MONTH_SALES)
    rows = session.execute(stmt, [current_month])
    for row in rows:
        print(f"=== Mes Actual: {row.month} ===")
        print(f"=========== Restaurante: {row.restaurant}")
        print(f"----------- Total Ventas: {row.total_sales}")
        print(f"------------------------------------------")

# Función 2
def get_current_month_sales_top(session):
    current_month = datetime.now().strftime('%B')
    log.info(f"Recuperando los 3 principales restaurantes de {current_month}")
    stmt = session.prepare(SELECT_CURRENT_MONTH_SALES_TOP)
    rows = session.execute(stmt, [current_month])
    for row in rows:
        print(f"=== Mes Actual: {row.month} ===")
        print(f"=========== Restaurante: {row.restaurant}")
        print(f"----------- Total Ventas: {row.total_sales}")
        print(f"------------------------------------------")

# Función 3
def get_all_sales(session):
    log.info("Recuperando todos los datos de ventas mensuales")
    stmt = session.prepare(SELECT_ALL_MONTHLY_SALES)
    rows = session.execute(stmt)

    for row in rows:
        print(f"=== Mes: {row.month} ===")
        print(f"----------- Restaurante: {row.restaurant}")
        print(f"----------- Total Ventas: {row.total_sales}")
        print(f"------------------------------------------")

# Función 4
def get_sales_by_month(session, month):
    log.info(f"Recuperando todas las ventas de {month}")
    
    # Diccionario
    month_map = {
        # Español a Inglés
        'enero': 'January', 'febrero': 'February', 'marzo': 'March',
        'abril': 'April', 'mayo': 'May', 'junio': 'June',
        'julio': 'July', 'agosto': 'August', 'septiembre': 'September',
        'octubre': 'October', 'noviembre': 'November', 'diciembre': 'December',
        # Meses en inglés
        'january': 'January', 'february': 'February', 'march': 'March',
        'april': 'April', 'may': 'May', 'june': 'June',
        'july': 'July', 'august': 'August', 'september': 'September',
        'october': 'October', 'november': 'November', 'december': 'December'
    }
    month_en = month_map.get(month.lower())
    
    if not month_en:
        try:
            month_num = int(month)
            if 1 <= month_num <= 12:
                month_en = datetime(2024, month_num, 1).strftime('%B')
            else:
                print("Número de mes no válido. Introduzca un número entre 1 y 12.")
                return
        except ValueError:
            print("Entrada de mes no válida. Introduzca el nombre del mes en español o inglés, o un número (1-12).")
            return

    stmt = session.prepare(SELECT_MONTHLY_SALES)
    rows = session.execute(stmt, [month_en])
    
    found_records = False
    print(f"\n=== Mes: {month_en} ===")
    for row in rows:
        found_records = True
        print(f"- Restaurante: {row.restaurant}")
        print(f"- Total Ventas: ${row.total_sales:,.2f}")
        print(f"------------------------------------------")
    
    if not found_records:
        print(f"No se encontraron registros de ventas para {month_en}")

# Función 5
def get_sales_by_restaurant(session, restaurant):
    log.info(f"Recuperando ventas mensuales para el restaurante: {restaurant}")
    stmt = session.prepare(SELECT_RESTAURANT_SALES)
    rows = session.execute(stmt, [restaurant])
    
    found_records = False
    print(f"=== Restaurante: {restaurant} ===")
    for row in rows:
        found_records = True
        print(f"- Mes: {row.month}")
        print(f"- Total Ventas: {row.total_sales}")
        print(f"------------------------------------------")
    
    if not found_records:
        print(f"No se encontraron registros de ventas para el restaurante: {restaurant}")

# Función 6
def get_sales_by_restaurant_and_month(session, restaurant, month):
    log.info(f"Recuperando ventas para {restaurant} en {month}")
    
    # Mapeo de los meses en español a inglés
    month_map = {
        'enero': 'January', 'febrero': 'February', 'marzo': 'March',
        'abril': 'April', 'mayo': 'May', 'junio': 'June',
        'julio': 'July', 'agosto': 'August', 'septiembre': 'September',
        'octubre': 'October', 'noviembre': 'November', 'diciembre': 'December'
    }
    
    # Obtener el nombre del mes en inglés
    month_en = month_map.get(month.lower())
    
    if not month_en:
        print("Entrada de mes no válida.")
        return
    
    # Preparar la consulta
    stmt = session.prepare(SELECT_MONTHLY_RESTAURANT_SALES)
    
    # Ejecutar la consulta
    rows = session.execute(stmt, [restaurant, month_en])
    
    # Verificar si hay resultados
    if not rows:
        print("No se encontraron ventas para el restaurante y mes indicados.")
        return
    
    # Imprimir la información de la primera fila
    for row in rows:
        print(f"=== Mes: {row.month} ===")
        print(f"=== Restaurante: {row.restaurant} ===")
        print(f"----------- Total Ventas: {row.total_sales}")
        print(f"------------------------------------------")

# Función 7
def get_sales_by_sales_range(session, min_sales, max_sales):
    log.info(f"Recuperando ventas en el rango {min_sales} a {max_sales}")
    
    # Retrieve sales for each month
    month_order = ['January', 'February', 'March', 'April', 'May', 'June', 
                   'July', 'August', 'September', 'October', 'November', 'December']
    
    for month in month_order:
        stmt = session.prepare(SELECT_SALES_IN_RANGE)
        rows = session.execute(stmt, [month, float(min_sales), float(max_sales)])
        
        for row in rows:
            print(f"- Mes: {row.month}")
            print(f"- Restaurante: {row.restaurant}")
            print(f"- Total Ventas: {row.total_sales}")
            print(f"------------------------------------------")

# Subir datos
def load_csv_to_cassandra(session, csv_file_path):
    log.info(f"Cargando datos desde {csv_file_path}")

    # Mapeo de meses en español a inglés
    month_map = {
        'enero': 'January', 'febrero': 'February', 'marzo': 'March',
        'abril': 'April', 'mayo': 'May', 'junio': 'June',
        'julio': 'July', 'agosto': 'August', 'septiembre': 'September',
        'octubre': 'October', 'noviembre': 'November', 'diciembre': 'December'
    }

    # Meses en orden para mapear índices de ventas
    month_order = ['January', 'February', 'March', 'April', 'May', 'June', 
                   'July', 'August', 'September', 'October', 'November', 'December']

    # Insert statements
    insert_statements = {
        'monthly': session.prepare("""
            INSERT INTO sales_by_month (month, restaurant, total_sales)
            VALUES (?, ?, ?)
        """),
        'restaurant': session.prepare("""
            INSERT INTO sales_by_restaurant (restaurant, month, total_sales)
            VALUES (?, ?, ?)
        """),
        'total': session.prepare("""
            INSERT INTO sales_by_total (month, total_sales, restaurant)
            VALUES (?, ?, ?)
        """)
    }

    try:
        with open(csv_file_path, 'r') as file:
            reader = csv.DictReader(file)
            for row in reader:
                restaurant = row.get('nombre', 'Restaurante Desconocido')
                
                # Procesar la lista de ventas como lista de enteros
                ventas_list = eval(row.get('ventas', '[]'))
                
                # Insertar ventas para cada mes
                for month_index, total_sales in enumerate(ventas_list):
                    month = month_order[month_index]
                    
                    # Insertar en las tablas
                    batch_data = [
                        (insert_statements['monthly'], [month, restaurant, float(total_sales)]),
                        (insert_statements['restaurant'], [restaurant, month, float(total_sales)]),
                        (insert_statements['total'], [month, float(total_sales), restaurant])
                    ]

                    for prepared_stmt, values in batch_data:
                        session.execute(prepared_stmt, values)

        log.info("Datos cargados exitosamente en Cassandra.")
        print("Importación de datos completada exitosamente.")
        
    except Exception as e:
        log.error(f"Error al cargar los datos: {str(e)}")
        print(f"Error al cargar los datos: {str(e)}")

def drop_data(session):
    log.info("Eliminando todas las tablas para limpiar los datos")
    tables_to_drop = [
        'sales_by_restaurant',
        'sales_by_month',
        'sales_by_total'
    ]
    
    for table in tables_to_drop:
        try:
            session.execute(f"DROP TABLE IF EXISTS {table}")
            log.info(f"Tabla eliminada: {table}")
        except Exception as e:
            log.error(f"Error al eliminar la tabla {table}: {str(e)}")
    
    create_schema(session)
    print("Data de Cassandra eliminada")