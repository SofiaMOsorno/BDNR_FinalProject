#!/usr/bin/env python3
import csv
from pymongo import MongoClient
import random

# Conexión a la base de datos
client = MongoClient("mongodb://localhost:27017/")
db = client["PFmongodb"]

# Nombres de las colecciones
ZONAS_COLLECTION = "zonas"
RESTAURANTES_COLLECTION = "restaurantes"

# Leer y cargar zonas en la colección
def load_zonas(file_path):
    with open(file_path, mode="r", encoding="utf-8") as file:
        reader = csv.DictReader(file)
        zonas = [{"id": int(row["id"]), "nombre": row["nombre"]} for row in reader]
        db[ZONAS_COLLECTION].insert_many(zonas)
        print(f"Datos de MongoDB creados: {len(zonas)} zonas cargadas.")


# Leer y cargar restaurantes en la colección
def load_restaurantes(file_path):
    zonas = list(db[ZONAS_COLLECTION].find())
    zona_ids = [zona["id"] for zona in zonas]

    with open(file_path, mode="r", encoding="utf-8") as file:
        reader = csv.DictReader(file)
        restaurantes = []

        for row in reader:
            # Agregar un campo zona_id aleatorio basado en los IDs de las zonas cargadas
            zona_id = random.choice(zona_ids)
            restaurante = {
                "id": int(row["id"]),
                "nombre": row["nombre"],
                "categoria": row["categoria"],
                "rating": float(row["rating"]),
                "ventas": list(map(int, row["ventas"][1:-1].split(", "))),  # Procesar la lista de ventas
                "zona_id": zona_id,
            }
            restaurantes.append(restaurante)

        db[RESTAURANTES_COLLECTION].insert_many(restaurantes)
        print(f"Datos de MongoDB creados: {len(restaurantes)} restaurantes cargados.")


# Borrar colecciones (opcional, para limpiar la base de datos antes de cargar)
def clear_collections():
    db[ZONAS_COLLECTION].delete_many({})
    db[RESTAURANTES_COLLECTION].delete_many({})
    print("Colecciones limpiadas.")


# Agregar índices después de cargar los datos
def create_indexes(db):
    """
    Crea índices para optimizar las consultas.
    """
    db["restaurantes"].create_index([("categoria", 1)])  # Índice en 'categoria'
    db["restaurantes"].create_index([("rating", -1)])    # Índice en 'rating'
    db["restaurantes"].create_index([("zona_id", 1)])    # Índice en 'zona_id'



# Llamar a create_indexes al final del archivo populate.py:
def main():
    clear_collections()  # Comentar si no deseas limpiar antes de cargar
    load_zonas("zonas.csv")
    load_restaurantes("restaurantes.csv")
    create_indexes(db)  # Crear índices después de cargar datos


if __name__ == "__main__":
    main()
