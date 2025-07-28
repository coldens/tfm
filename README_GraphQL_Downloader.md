# Pebble Device Data Importer

Script robusto para descargar y almacenar todos los registros de `pebble_device_record` desde la API GraphQL de IoTeX directamente en MongoDB.

## Archivo incluido

- **`download_to_mongodb.py`** — Importador concurrente y tolerante a fallos para descargar y guardar datos en MongoDB.

## Instalación

1. Clona este repositorio y entra en la carpeta del proyecto.
2. (Opcional pero recomendado) Crea un entorno virtual:

```bash
python3 -m venv .venv
source .venv/bin/activate
```

3. Instala los paquetes requeridos:

```bash
pip install requests urllib3 pymongo python-dotenv python-dateutil
```

Asegúrate de tener MongoDB en ejecución y accesible desde la URI configurada.

## Configuración

El script utiliza variables de entorno (puedes definirlas en un archivo `.env` en la raíz):

- `MONGO_URI` — URI de conexión a MongoDB (por defecto: `mongodb://localhost:27017`)
- `DB_NAME` — Nombre de la base de datos (por defecto: `pebble-dataset`)
- `RECORDS_COLLECTION` — Colección donde se guardan los registros (por defecto: `pebble_device_record`)
- `CHUNK_SIZE` — Tamaño de lote para cada petición a la API (por defecto: `1000`)
- `MAX_WORKERS` — Número de hilos paralelos (por defecto: núcleos de CPU o 4)
- `TO_CREATED_AT` — Fecha límite superior para descargar registros (por defecto: `2025-01-01T00:00:00Z`)
- `POOL_CONNECTIONS`, `POOL_MAXSIZE` — Parámetros avanzados de conexión HTTP (opcional)

Ejemplo de `.env`:

```
MONGO_URI=mongodb://localhost:27017
DB_NAME=pebble-dataset
RECORDS_COLLECTION=pebble_device_record
CHUNK_SIZE=1000
MAX_WORKERS=8
TO_CREATED_AT=2025-01-01T00:00:00Z
```

## Uso

Ejecuta el script principal:

```bash
python download_to_mongodb.py
```

**Características principales:**

- Descarga todos los campos de `pebble_device_record` usando paginación.
- Inserta los datos directamente en MongoDB.
- Paralelización con ThreadPoolExecutor (número de hilos configurable).
- Reanuda automáticamente desde el último registro descargado (según `created_at`).
- Manejo robusto de errores y reintentos automáticos.
- Soporta parada segura con Ctrl+C (graceful shutdown).
- Logging detallado del progreso y errores.

## Campos descargados

El script descarga todos los campos relevantes:

- `id` — ID único del registro
- `imei` — IMEI del dispositivo
- `created_at` — Fecha de creación
- `accelerometer` — Datos del acelerómetro
- `gas_resistance` — Resistencia del gas
- `gyroscope` — Datos del giroscopio
- `humidity` — Humedad
- `latitude` — Latitud GPS
- `light` — Sensor de luz
- `longitude` — Longitud GPS
- `operator` — Operador de red
- `pressure` — Presión atmosférica
- `signature` — Firma criptográfica
- `snr` — Relación señal-ruido
- `temperature` — Temperatura (sensor 1)
- `temperature2` — Temperatura (sensor 2)
- `timestamp` — Timestamp Unix
- `updated_at` — Fecha de actualización
- `vbat` — Voltaje de batería

## Funcionamiento y flujo

1. El script consulta la API GraphQL de IoTeX usando lotes (`chunk_size`) y paginación.
2. Los datos se insertan en MongoDB, convirtiendo automáticamente los campos de fecha.
3. Si se interrumpe el proceso, al reiniciar reanuda desde el último `created_at` almacenado.
4. El proceso continúa hasta que no hay más datos nuevos o se alcanza la fecha límite (`TO_CREATED_AT`).

## Monitoreo y logs

- Progreso de descarga y número total de registros procesados.
- Mensajes de advertencia y error en caso de fallos de red, API o base de datos.
- Mensaje final con el total de registros importados.

## Ejemplo de salida

```
2024-06-01 12:00:00,000 - INFO - 🔄 Starting import with chunks of 1000 records
2024-06-01 12:00:00,001 - INFO - Using 8 workers
2024-06-01 12:00:01,234 - INFO - Offset 0: 1000/1000 records processed
2024-06-01 12:00:02,345 - INFO - Offset 1000: 1000/1000 records processed
...
2024-06-01 12:10:00,000 - INFO - ✅ Import completed. Total records processed: 50000
```

## Requisitos

- Python 3.8+
- MongoDB (local o remoto)

## Notas adicionales

- El script ignora certificados SSL (útil para desarrollo, no recomendado en producción).
- Si la API impone límites de rate limiting, el script reintenta automáticamente.
- Los campos de fecha se almacenan como objetos de fecha en MongoDB.
- Puedes modificar los parámetros de conexión y rendimiento vía `.env` o variables de entorno.

---

Para cualquier duda o mejora, abre un issue o contacta al autor.
