import pydgraph
import csv
import os
import json
import random
import unicodedata

def configurar_esquema(client):
    schema = """

    type Users {
    Name
    Email
    ciudad
    sigue_user
    sigue_restaurantes
    
    }

    type Restaurant {
        restaurant_name
        categoria
        rating
        followers
        esta_en
    }

    type City {
        City_name
    }

    # Definición de los tipos para cada predicado
    Name: string @index(exact) .
    Email: string @index(exact) .
    seguidores: int .         
    ciudad: [uid] .
    sigue_user: [uid] .              
    sigue_restaurantes: [uid] .            
    

    restaurant_name: string @index(exact) .
    categoria: [string] @index(term) .
    rating: float .
    followers: [uid] @reverse @count .      
    esta_en: [uid] @reverse .

    City_name: string @index(term) .
    
    """

    return client.alter(pydgraph.Operation(schema=schema))


def normalizeString(string):
    if string=='Ciudad de Mexico':
        return string
    
    normalized = ''.join(
        char for char in unicodedata.normalize('NFD', string)
        if unicodedata.category(char) != 'Mn'
    )
    # Capitalizar el resultado
    return normalized.capitalize()

def cargar_datos(csv_file):
    datos = []
    if os.path.exists(csv_file):
        with open(csv_file, mode='r', encoding='utf-8') as file:
            reader = csv.DictReader(file)
            for row in reader:
                datos.append(row)
    return datos




def procesar_usuarios(csvfile):
    # Leer los datos del CSV
    usuariosruta = csvfile
    datos = cargar_datos(usuariosruta)

    usuarios = []
    for dato in datos:
        # Crear el usuario
        usuario = {
            "uid": f"_:user{dato.get('id', 0)}",  # Asignar un UID dinámico, pero será el mismo para las relaciones
            "Name": dato.get("nombre"),
            "Email": dato.get("email"),
            "Seguidores": int(dato.get("seguidores", 0)),
            "Ciudad": dato.get("zona"),
            'sigue_user': [],
            'sigue_restaurantes': []
        }
        usuarios.append(usuario)

    return usuarios


def procesar_restaurantes(csvfile):
    datos = cargar_datos(csvfile)
    restaurantes = []
    for dato in datos:
        restaurante = {
            'uid': f"_:restaurante{dato.get('id', 0)}",  # Asignar un UID dinámico
            "restaurant_name": dato.get('nombre'),
            'categoria': dato.get('categoria'),
            'rating': float(dato.get('rating')),
            'followers': [],
            "esta_en": []  # Se rellenará con zonas después
        }
        restaurantes.append(restaurante)

    
    return restaurantes


def procesar_zonas(csvfile):
    datos = cargar_datos(csvfile)
    zonas = []
    for dato in datos:
        zona = {
            'uid': f"_:city{dato.get('id')}",  # Asignar un UID dinámico para cada zona
            'City_name': dato.get('nombre'),
            "restaurantes": []  # Se rellenará con restaurantes después
        }
        zonas.append(zona)

    
    return zonas


def crear_relaciones(usuarios, restaurantes, zonas):
    # Asignar zona a cada restaurante
    for restaurante in restaurantes:
        # Seleccionar una zona aleatoria
        zona_asignada = random.choice(zonas)
        
        # Relacionar restaurante con su zona mediante el uid
        restaurante["esta_en"] = zona_asignada["uid"]
        
        # Añadir el restaurante a la lista de restaurantes en esa zona
        zona_asignada["restaurantes"].append(restaurante["uid"])
        
        # Inicializar la lista de seguidores del restaurante
        restaurante["followers"] = []

    # Asignar relaciones para cada usuario
    for usuario in usuarios:
        # Asignar a 5 usuarios que sigue (si hay suficientes usuarios)
        usuarios_a_seguir = random.sample([u for u in usuarios if u["uid"] != usuario["uid"]], min(5, len(usuarios) - 1))
        usuario["sigue_user"] = [u["uid"] for u in usuarios_a_seguir]  # Relacionar usuario con otros usuarios

        # Asignar a 6 restaurantes que sigue
        restaurantes_a_seguir = random.sample(restaurantes, min(6, len(restaurantes)))
        usuario["sigue_restaurantes"] = [r["uid"] for r in restaurantes_a_seguir]  # Relacionar usuario con restaurantes

        # Actualizar los restaurantes con los seguidores (usuarios que siguen el restaurante)
        for restaurante in restaurantes_a_seguir:
            restaurante["followers"].append(usuario["uid"])  # Añadir usuario como seguidor del restaurante

        # Asignar al usuario una zona aleatoria
        zona_asignada = random.choice(zonas)
        usuario["Ciudad"] = zona_asignada["uid"]  # Relacionar usuario con su zona

    return usuarios, restaurantes, zonas


def agregar_datos(client, usuarios, restaurantes, zonas):
    txn = client.txn()

    # Crear una lista de mutaciones
    nodos = []

    # Mutaciones para usuarios
    for usuario in usuarios:
        nodo_usuario = {
            "uid": usuario["uid"],  # Usar el UID existente
            "Name": usuario["Name"],
            "Email": usuario["Email"],
            "Ciudad": {"uid": usuario.get("Ciudad")},  # Relación con zona
            "sigue_user": [{"uid": uid} for uid in usuario.get("sigue_user", [])],  # Relación con otros usuarios
            "sigue_restaurantes": [{"uid": uid} for uid in usuario.get("sigue_restaurantes", [])],  # Relación con restaurantes
        }
        nodos.append(nodo_usuario)

    # Mutaciones para restaurantes
    for restaurante in restaurantes:
        nodo_restaurante = {
            "uid": restaurante["uid"],  # Usar el UID existente
            "restaurant_name": restaurante["restaurant_name"],
            "categoria": restaurante["categoria"],
            "rating": restaurante["rating"],
            "esta_en": {"uid": restaurante.get("esta_en")},  # Relación con la zona
            "followers": [{"uid": follower_uid} for follower_uid in restaurante.get("followers", [])],  # Relación con seguidores
        }
        nodos.append(nodo_restaurante)

    # Mutaciones para zonas
    for zona in zonas:
        nodo_zona = {
            "uid": zona["uid"],  # Usar el UID existente
            "City_name": zona["City_name"],
            "restaurantes": [{"uid": uid} for uid in zona.get("restaurantes", [])],  # Relación con restaurantes
        }
        nodos.append(nodo_zona)

    # Crear la mutación en lote
    response = txn.mutate(set_obj=nodos)
    

    # Confirmar la transacción
    txn.commit()
    print("Data de dgraph creada")



def Top_3_restaurants_by_followers(client, order):
    query = """
    {
        all(func: has(restaurant_name)) {
            restaurant_name
            followers_count: count(followers)
        }
    }
    """
    try:
        # Ejecuta la consulta
        res = client.txn(read_only=True).query(query)

        # Decodifica la respuesta JSON
        response = json.loads(res.json)
    except Exception as e:
        print(f"Error durante la consulta o decodificación JSON: {e}")
        return  # Termina si ocurre un error

    # Extrae los datos de restaurantes
    restaurants = response.get("all", [])
    if not restaurants:
        print("No se encontraron restaurantes.")
        return  # Termina si la lista está vacía

    # Ordena los restaurantes según el orden solicitado por el usuario
    if order == "asc":
        
        sorted_restaurants = sorted(restaurants, key=lambda r: r.get('followers_count', 0), reverse=True)
        
        
    elif order == "desc":
        
        sorted_restaurants = sorted(restaurants, key=lambda r: r.get('followers_count', 0))
        
        

    # Obtén los top 3
    top_3 = sorted_restaurants[:3]

    # Muestra los resultados
    print('-'*40)
    print(f"Top 3 restaurantes con {'menor' if order == 'desc' else 'mayor'} número de seguidores:")
    for idx, restaurant in enumerate(top_3, start=1):
        name = restaurant.get("restaurant_name", "Desconocido")
        followers = restaurant.get("followers_count", 0)
        print(f"{idx}. {name} - {followers} seguidores")
    print('-'*40)

    return top_3

def get_restaurants_by_city(client, city_name):
    
    city_name=normalizeString(city_name)

    query = """
    
    query RestaurantsInCity($city_name: string) {
    city(func: allofterms(City_name, $city_name)) {
        City_name
        ~esta_en { 
            restaurant_name
            cant_followers: count(followers)
            }
        }
    }

    """
    

    variables = {"$city_name": city_name}
    res = client.txn(read_only=True).query(query, variables=variables)
    data = json.loads(res.json)

    
    city_info = data.get("city", [])


    if not city_info:
        print(f"No se encontró la ciudad con el nombre: {city_name}")
        return []
    
    

    # Obtener la lista de restaurantes
    restaurants = city_info[0].get("~esta_en", [])
    restaurant_list = [
        {"name": r.get("restaurant_name"), "followers": r.get("cant_followers", 0)}
        for r in restaurants
    ]

    if restaurant_list:
        print(f"Restaurantes en {city_name}:")
        print("-" * 40)
        for idx, restaurant in enumerate(restaurant_list, start=1):
            print(f"{idx}. {restaurant['name']} - {restaurant['followers']} seguidores")
        print("-" * 40)
    else:
        print(f"No se encontraron restaurantes en la ciudad: {city_name}")
        
        return restaurant_list
    
def drop_all(client):
    print("Data de dgraph eliminada")
    return client.alter(pydgraph.Operation(drop_all=True))