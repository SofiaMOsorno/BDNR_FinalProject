import os
import logging
import pydgraph
from pymongo import MongoClient
from cassandra.cluster import Cluster
import modeldgraph
import modelpython
import modelcassandra
import populate

# Configuración de URLs y constantes
DGRAPH_URI = os.getenv('DGRAPH_URI', 'localhost:9080')
MONGODB_URI = os.getenv('MONGODB_URI', 'mongodb://localhost:27017')
CASSANDRA_CLUSTER_IPS = os.getenv('CASSANDRA_CLUSTER_IPS', 'localhost')
CASSANDRA_KEYSPACE = os.getenv('CASSANDRA_KEYSPACE', 'investments')
CASSANDRA_REPLICATION_FACTOR = os.getenv('CASSANDRA_REPLICATION_FACTOR', '1')
DB_NAME = "PFmongodb"

# Configurar el logger
log = logging.getLogger()
log.setLevel('INFO')
handler = logging.FileHandler('unified_system.log')
handler.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(name)s: %(message)s"))
log.addHandler(handler)


# Menú principal
def print_main_menu():
    mm_options = {
        1: "Crear datos para todas las bases de datos",
        2: "Eliminar datos de todas las bases de datos",
        3: "Consultar de seguidores y restaurantes",
        4: "Consultar de rating de restaurantes",
        5: "Consultar de ventas",
        6: "Salir"
    }
    for key in mm_options.keys():
        print(key, '--', mm_options[key])


# Submenús
def print_dgraph_menu():
    dg_options = {
        1: "Top 3 restaurantes por número de seguidores",
        2: "Restaurantes por nombre de ciudad",
        3: "Volver al menú principal"
    }
    for key in dg_options.keys():
        print(key, '--', dg_options[key])


def print_mongo_menu():
    mongo_options = {
        1: "Top 3 restaurantes por zona",
        2: "Top 3 restaurantes por categoría",
        3: "Volver al menú principal"
    }
    for key in mongo_options.keys():
        print(key, '--', mongo_options[key])


def print_cassandra_menu():
    cass_options = {
        1: "Mostrar ventas del mes actual",
        2: "Mostrar las 3 principales ventas de este mes",
        3: "Consultar ventas mensuales",
        4: "Volver al menú principal"
    }
    for key in cass_options.keys():
        print(key, '--', cass_options[key])
        
def print_monthly_sales_menu():
    thm_options = {
        1: "Todas", # cada restaurante con cada total de ventas de cada mes o cada mes con cada total de ventas de ese mes por restaurante...
        2: "Buscar por Mes", # input = mes / output = cada restaurante con cada total de ventas de ese mes
        3: "Buscar por Restaurante", # input = restaurante / output = cada total de ventas mensual
        4: "Buscar por Restaurante y Mes", # input = restaurante, mes / output = el total de ese restaurante ese mes
        5: "Buscar por rango de ventas", # input = min, max / output = restaurantes que han ganado en ese rango y el mes en el que lo hicieron
        6: "Salir" 
    }
    for key in thm_options.keys():
        print('    ', key, '--', thm_options[key])


def main():
    # Configuración de clientes
    log.info("Conectando a las bases de datos...")
    # Configuración de Dgraph
    client_stub = pydgraph.DgraphClientStub(DGRAPH_URI)
    dgraph_client = pydgraph.DgraphClient(client_stub)
    modeldgraph.configurar_esquema(dgraph_client)

    # Configuración de MongoDB
    mongo_client = MongoClient(MONGODB_URI)
    mongo_database = mongo_client[DB_NAME]

    # Configuración de Cassandra
    cluster = Cluster(CASSANDRA_CLUSTER_IPS.split(','))
    cassandra_session = cluster.connect()
    modelcassandra.create_keyspace(cassandra_session, CASSANDRA_KEYSPACE, CASSANDRA_REPLICATION_FACTOR)
    cassandra_session.set_keyspace(CASSANDRA_KEYSPACE)
    modelcassandra.create_schema(cassandra_session)

    while True:
        print_main_menu()
        try:
            option = int(input("Ingrese su opción: "))
            if option == 1:
                # Crear datos en todas las bases
                usuarios = modeldgraph.procesar_usuarios("usuarios.csv")
                restaurantes = modeldgraph.procesar_restaurantes("restaurantes.csv")
                zonas = modeldgraph.procesar_zonas("zonas.csv")
                modeldgraph.crear_relaciones(usuarios, restaurantes, zonas)
                modeldgraph.agregar_datos(dgraph_client, usuarios, restaurantes, zonas)
                populate.main()
                modelcassandra.load_csv_to_cassandra(cassandra_session, "restaurantes.csv")
                print("Datos creados en todas las bases de datos.")
            elif option == 2:
                # Eliminar datos de todas las bases
                modeldgraph.drop_all(dgraph_client)
                modelpython.delete_all_data(mongo_database)
                modelcassandra.drop_data(cassandra_session)
                print("Datos eliminados de todas las bases de datos.")
            elif option == 3:
                # Submenú de Dgraph
                print_dgraph_menu()
                dg_option = int(input("Ingrese su opción: "))
                if dg_option == 1:
                    order = input("¿Top 3 con más seguidores (asc) o con menos seguidores (desc)?: ").lower()
                    modeldgraph.Top_3_restaurants_by_followers(dgraph_client, order)
                elif dg_option == 2:
                    city_name = input("Ingrese el nombre de la ciudad: ")
                    modeldgraph.get_restaurants_by_city(dgraph_client, city_name)
            elif option == 4:
                # Submenú de MongoDB
                print_mongo_menu()
                mongo_option = int(input("Ingrese su opción: "))
                if mongo_option == 1:
                    zone_name = input("Ingrese el nombre de la zona (o deje en blanco para todos): ")
                    modelpython.top_restaurants_by_zone(mongo_database, show_all=not bool(zone_name), zone_name=zone_name)
                elif mongo_option == 2:
                    category = input("Ingrese la categoría: ")
                    modelpython.top_restaurants_by_category(mongo_database, category)
            elif option == 5:
                # Submenú de Cassandra
                print_cassandra_menu()
                cass_option = int(input("Ingrese su opción: "))
                if cass_option == 1:
                    modelcassandra.get_current_month_sales(cassandra_session)
                elif cass_option == 2:
                    modelcassandra.get_current_month_sales_top(cassandra_session)
                elif cass_option == 3:
                    # Menú mensual de Cassandra
                    print_monthly_sales_menu()
                    tv_option = int(input('Ingrese su preferencia de filtro: '))
                    if tv_option == 1:
                        modelcassandra.get_all_sales(cassandra_session)
                    elif tv_option == 2:
                        month = input('Mes: ')
                        modelcassandra.get_sales_by_month(cassandra_session, month)
                    elif tv_option == 3:
                        restaurant = input('Restaurante: ')
                        modelcassandra.get_sales_by_restaurant(cassandra_session, restaurant)
                    elif tv_option == 4:
                        restaurant = input('Restaurante: ')
                        month = input('Mes: ')
                        modelcassandra.get_sales_by_restaurant_and_month(cassandra_session, restaurant, month)
                    elif tv_option == 5:
                        min_value = input('Ventas mínimas: ')
                        max_value = input('Ventas máximas: ')
                        modelcassandra.get_sales_by_sales_range(cassandra_session, min_value, max_value)
            elif option == 6:
                print("Cerrando conexiones...")
                close_client_stub(client_stub)
                cluster.shutdown()
                mongo_client.close()
                print("Adiós.")
                break
            else:
                print("Opción no válida.")
        except ValueError as e:
            print("Entrada no válida:", e)


# Dgraph: Cerrar cliente stub
def close_client_stub(client_stub):
    client_stub.close()


if __name__ == "__main__":
    main()
