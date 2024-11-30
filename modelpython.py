from pymongo import MongoClient

def delete_all_data(database):
    """
    Elimina todas las colecciones de la base de datos.
    """
    database["zonas"].delete_many({})
    database["restaurantes"].delete_many({})
    print("Data de MongoDB eliminada")


def top_restaurants_by_zone(database, show_all=True, zone_name=None):
    zonas_collection = database["zonas"]
    restaurantes_collection = database["restaurantes"]

    if show_all:
        # Obtener las primeras 5 zonas con un pipeline de agregación
        zonas = zonas_collection.aggregate([
            {"$limit": 5},  # Limitar a las primeras 5 zonas
            {"$lookup": {   # Unir restaurantes relacionados
                "from": "restaurantes",
                "localField": "id",
                "foreignField": "zona_id",
                "as": "restaurantes"
            }},
            {"$project": {
                "nombre": 1,
                "restaurantes": {"$slice": ["$restaurantes", 3]}  # Limitar a los 3 mejores
            }}
        ])
        for zona in zonas:
            print(f"Zona: {zona['nombre']}")
            for restaurante in zona["restaurantes"]:
                print(f"- {restaurante['nombre']} (Rating: {restaurante['rating']})")
    else:
        if not zone_name:
            print("Debes proporcionar el nombre de la zona.")
            return
        zona = zonas_collection.find_one({"nombre": zone_name})
        if not zona:
            print(f"No se encontró la zona con el nombre '{zone_name}'.")
            return

        # Usar agregación para obtener el top 3 de restaurantes en esa zona
        restaurantes = restaurantes_collection.aggregate([
            {"$match": {"zona_id": zona["id"]}},  # Filtrar por zona específica
            {"$sort": {"rating": -1}},           # Ordenar por rating descendente
            {"$limit": 3}                        # Limitar a los 3 mejores
        ])
        print(f"Zona: {zona['nombre']}")
        for restaurante in restaurantes:
            print(f"- {restaurante['nombre']} (Rating: {restaurante['rating']})")



def top_restaurants_by_category(db, category):
    """
    Muestra el top 3 de restaurantes según la categoría.
    """
    # Asegurar la consulta insensible a mayúsculas/minúsculas
    restaurantes = db["restaurantes"].find(
        {"categoria": {"$regex": f"^{category}$", "$options": "i"}}
    ).sort("rating", -1).limit(3)

    # Contar los resultados
    num_restaurantes = db["restaurantes"].count_documents(
        {"categoria": {"$regex": f"^{category}$", "$options": "i"}}
    )

    if num_restaurantes == 0:
        print(f"No se encontraron restaurantes en la categoría '{category}'.")
        return

    print(f"Top 3 restaurantes en la categoría '{category}':")
    for restaurante in restaurantes:
        print(f"- {restaurante['nombre']} (Rating: {restaurante['rating']})")
