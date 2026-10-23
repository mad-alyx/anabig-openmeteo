from kafka import KafkaProducer
import requests
import json
import time
from datetime import datetime

producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

LOKASI = "Pontianak, Kalimantan Barat"
LATITUDE = -0.0263
LONGITUDE = 109.3425

url_weather = f"https://api.open-meteo.com/v1/forecast?latitude={LATITUDE}&longitude={LONGITUDE}&current=temperature_2m,relative_humidity_2m,wind_speed_10m"
url_airquality = f"https://air-quality-api.open-meteo.com/v1/air-quality?latitude={LATITUDE}&longitude={LONGITUDE}&current=pm2_5,pm10,carbon_monoxide,nitrogen_dioxide,ozone,us_aqi"

def kategori_aqi(aqi):
    if aqi <= 50: return "Baik"
    elif aqi <= 100: return "Sedang"
    elif aqi <= 150: return "Tidak sehat (kelompok sensitif)"
    elif aqi <= 200: return "Tidak sehat"
    elif aqi <= 300: return "Sangat tidak sehat"
    else: return "Berbahaya"

while True:
    # Ambil data cuaca
    data_cuaca = requests.get(url_weather).json()["current"]
    pesan_cuaca = {
        "lokasi": LOKASI,
        "suhu": data_cuaca["temperature_2m"],
        "kelembapan": data_cuaca["relative_humidity_2m"],
        "kecepatan_angin": data_cuaca["wind_speed_10m"],
        "waktu": data_cuaca["time"]
    }
    producer.send("data-cuaca", value=pesan_cuaca)

    # Ambil data kualitas udara
    data_udara = requests.get(url_airquality).json()["current"]
    pesan_udara = {
        "lokasi": LOKASI,
        "pm2_5": data_udara["pm2_5"],
        "pm10": data_udara["pm10"],
        "carbon_monoxide": data_udara["carbon_monoxide"],
        "nitrogen_dioxide": data_udara["nitrogen_dioxide"],
        "ozone": data_udara["ozone"],
        "us_aqi": data_udara["us_aqi"],
        "waktu": data_udara["time"]
    }
    producer.send("data-kualitas-udara", value=pesan_udara)

    # Print rapi
    jam = datetime.now().strftime("%H:%M:%S")
    print(f"\n[{jam}] — {LOKASI}")
    print("─" * 45)
    print(f"  CUACA")
    print(f"  Suhu          : {pesan_cuaca['suhu']} °C")
    print(f"  Kelembapan    : {pesan_cuaca['kelembapan']} %")
    print(f"  Kec. Angin    : {pesan_cuaca['kecepatan_angin']} km/h\n")
    print(f"  KUALITAS UDARA")
    print(f"  PM2.5         : {pesan_udara['pm2_5']} μg/m³")
    print(f"  PM10          : {pesan_udara['pm10']} μg/m³")
    print(f"  US AQI        : {pesan_udara['us_aqi']} ({kategori_aqi(pesan_udara['us_aqi'])})")
    print("─" * 45)

    time.sleep(10)