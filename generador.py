import csv
import random
from faker import Faker

# Inicializamos Faker para generar datos aleatorios
faker = Faker()

# Definir configuraciones para los datos
CATEGORIAS = ["sushi", "italiana", "francesa", "mexicana", "china"]
CIUDADES = ["Guadalajara", "Ciudad de Mexico", "Monterrey", "Cancun", "Tijuana"]
MESES = ["enero", "febrero", "marzo", "abril", "mayo", "junio",
         "julio", "agosto", "septiembre", "octubre", "noviembre", "diciembre"]
NUM_RESTAURANTES = 30
NUM_USUARIOS = 50
NUM_ZONAS = 5  # Número de zonas únicas

# Generar las zonas (nombres de ciudades sin repetición)
zonas = [{"id": i + 1, "nombre": ciudad} for i, ciudad in enumerate(random.sample(CIUDADES, NUM_ZONAS))]

# Generar los restaurantes
restaurantes = []
for i in range(NUM_RESTAURANTES):
    restaurante = {
        "id": i + 1,
        "nombre": faker.company(),
        "categoria": random.choice(CATEGORIAS),
        "rating": round(random.uniform(3.0, 5.0), 1),
        "ventas": [random.randint(50000, 1000000) for _ in range(12)]  # Ventas para 12 meses
    }
    restaurantes.append(restaurante)

# Generar los usuarios
usuarios = []
for i in range(NUM_USUARIOS):
    usuario = {
        "id": i + 1,
        "nombre": faker.name(),
        "email": faker.email(),
        "seguidores": random.randint(1, 1000),
        "zona": random.choice(zonas)["nombre"]  # Asignar zona aleatoria de las 5 ciudades
    }
    usuarios.append(usuario)

# Función para escribir CSV
def write_csv(file_name, data, header):
    with open(file_name, mode="w", newline="", encoding="utf-8") as file:
        writer = csv.DictWriter(file, fieldnames=header)
        writer.writeheader()
        writer.writerows(data)

# Escribir los archivos CSV
write_csv("restaurantes.csv", restaurantes, ["id", "nombre", "categoria", "rating", "ventas"])
write_csv("zonas.csv", zonas, ["id", "nombre"])
write_csv("usuarios.csv", usuarios, ["id", "nombre", "email", "seguidores", "zona"])

"Archivos CSV generados exitosamente."
