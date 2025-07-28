#!/usr/bin/env python3
"""
Script para exportar datos de la colección MongoDB a CSV plano usando csv estándar de Python y tqdm para barra de progreso.

Requisitos:
    pip install pymongo tqdm
"""
from pymongo import MongoClient
from os import path
import csv
from tqdm import tqdm
from datetime import datetime

# Configuración de conexión
MONGO_URI = "mongodb://localhost:27017/"
DB_NAME = "pebble-dataset"
COLLECTION_NAME = "pebble_records_unique"
OUTPUT_CSV = path.join(path.dirname(__file__), "exported", "pebble-dataset-2022.csv")


def main():
    client = MongoClient(MONGO_URI)
    db = client[DB_NAME]
    collection = db[COLLECTION_NAME]

    # Definir el rango de fechas para 2022
    start_2022 = datetime(2022, 1, 1)
    end_2022 = datetime(2023, 1, 1)
    filtro_2022 = {"created_at": {"$gte": start_2022, "$lt": end_2022}}

    # Contar el número total de documentos para la barra de progreso SOLO 2022
    total = collection.count_documents(filtro_2022)
    if total == 0:
        print("No se encontraron registros para exportar en 2022.")
        return

    cursor = collection.find(filtro_2022)

    # Obtener los nombres de las columnas del primer documento
    first_doc = cursor.next()
    first_doc["_id"] = str(first_doc["_id"])
    first_doc["created_at"] = first_doc["created_at"].isoformat()
    first_doc["updated_at"] = first_doc["updated_at"].isoformat()
    fieldnames = list(first_doc.keys())

    with open(OUTPUT_CSV, mode="w", newline="", encoding="utf-8") as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()
        # Escribir el primer documento
        writer.writerow(first_doc)
        # Escribir el resto con barra de progreso
        for doc in tqdm(cursor, total=total-1, desc="Exportando registros"):
            doc["_id"] = str(doc["_id"])
            doc["created_at"] = doc["created_at"].isoformat()
            doc["updated_at"] = doc["updated_at"].isoformat()
            writer.writerow(doc)
    print(f"CSV exportado correctamente a {OUTPUT_CSV}")


if __name__ == "__main__":
    main()
