"""
Manejador Lambda para inferencia de anomalías con SageMaker.

Responsabilidades
- Validar y parsear el cuerpo de la petición (payload JSON) recibida por API Gateway.
- Extraer y preprocesar las 18 features requeridas por el modelo (acelerómetro, giroscopio y sensores ambientales).
- Normalizar el vector de features usando las medias y desviaciones estándar calculadas durante el entrenamiento (almacenadas en S3).
- Invocar el endpoint de SageMaker para obtener el score de anomalía.
- Persistir el registro original junto con su score en una tabla de Amazon DynamoDB.
- Publicar una alerta en Amazon SNS si el score supera un umbral configurable.
"""

import os
import json
import boto3
import datetime
import decimal
import ast
from decimal import Decimal

# --- init phase (runs once per container) -------------------------------
s3 = boto3.client("s3")
bucket, key = os.environ["STATS_S3_URI"].replace("s3://", "").split("/", 1)
stats_obj = json.loads(s3.get_object(Bucket=bucket, Key=key)["Body"].read())
FEATURES = list(stats_obj.keys())            # orden garantizado


def z(x, mean, std): return (x - mean) / std if std else 0.0


# clients
dynamo = boto3.resource("dynamodb").Table(os.environ["TABLE_NAME"])
sm_rt = boto3.client("sagemaker-runtime")
sns = boto3.client("sns")

EP = os.environ["ENDPOINT_NAME"]
TOPIC = os.environ["TOPIC_ARN"]
THRESH = float(os.getenv("THRESHOLD", "1.0711"))

# ------------------------------------------------------------------------


def handler(event, _ctx):
    body = json.loads(event["body"])

    # 1. extraer acelerómetro / giroscopio
    accel = ast.literal_eval(body["accelerometer"])
    gyro = ast.literal_eval(body["gyroscope"])

    # 2. convertir timestamp → hora y día_sem
    ts_dt = datetime.datetime.utcfromtimestamp(int(body["timestamp"]))
    hour = ts_dt.hour
    dow = ts_dt.weekday()

    raw = {
        "accel_x": accel[0], "accel_y": accel[1], "accel_z": accel[2],
        "gyro_x":  gyro[0],  "gyro_y":  gyro[1],  "gyro_z":  gyro[2],
        "temperature":  body["temperature"],
        "temperature2": body["temperature2"],
        "humidity":     body["humidity"],
        "pressure":     body["pressure"],
        "light":        body["light"],
        "gas_resistance": body["gas_resistance"],
        "snr":  body["snr"],
        "vbat": body["vbat"],
        "latitude":  float(body["latitude"]),
        "longitude": float(body["longitude"]),
        "hour": hour,
        "day_of_week": dow
    }

    # 3. z-score usando stats de entrenamiento
    vec = [z(raw[f], *stats_obj[f]) for f in FEATURES]
    payload = ",".join(f"{v:.6f}" for v in vec)

    # 4. inferencia
    resp = sm_rt.invoke_endpoint(
        EndpointName=EP,
        ContentType="text/csv",
        Accept="application/json",
        Body=payload
    )

    # ✅ extraer el score desde el JSON
    body_bytes = resp["Body"].read()
    score = json.loads(body_bytes)["scores"][0]["score"]

    # 5. guardar
    item = {
        "imei": body["imei"],
        "ingested_at": datetime.datetime.now(datetime.timezone.utc).isoformat(),
        "device_ts": Decimal(str(body["timestamp"])),
        "score": Decimal(str(score)),
        **{k: Decimal(str(v)) for k, v in raw.items()}
    }
    dynamo.put_item(Item=item)

    # 6. alerta
    if score > THRESH:
        sns.publish(TopicArn=TOPIC,
                    Subject="Pebble anomaly",
                    Message=f"IMEI {body['imei']} score {score:.3f}")

    return {"statusCode": 200,
            "body": json.dumps({"score": score, "ingested_at": item["ingested_at"]})}
