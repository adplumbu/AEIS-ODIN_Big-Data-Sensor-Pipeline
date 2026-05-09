# Big Data Sensor Pipeline

Proiect universitar — pipeline end-to-end pentru date de la senzori IoT de mediu.

```
Python Simulator ──► Kafka ──► Spark Structured Streaming ──► InfluxDB ──► Grafana
```

## Arhitectură

```
┌─────────────────────────────────────────────────────────────────────┐
│                         Docker Compose                              │
│                                                                     │
│  ┌──────────────┐    ┌──────────────────────────────────────────┐  │
│  │   Simulator  │    │              Kafka Cluster               │  │
│  │  (Python)    │───►│  Zookeeper + Kafka + Kafka UI (:8080)    │  │
│  │  8 senzori   │    │  Topic: sensor-data (3 partitions)       │  │
│  │  1 msg/sec   │    └──────────────┬───────────────────────────┘  │
│  └──────────────┘                   │                               │
│                                     ▼                               │
│                    ┌────────────────────────────┐                   │
│                    │    Spark Processor         │                   │
│                    │  Structured Streaming      │                   │
│                    │  - Parse JSON              │                   │
│                    │  - Anomaly detection       │                   │
│                    │  - Window aggregations     │                   │
│                    └────────────┬───────────────┘                   │
│                                 │                                   │
│                    ┌────────────▼───────────────┐                   │
│                    │       InfluxDB 2.x (:8086) │                   │
│                    │  measurement: sensor_reading│                   │
│                    │  measurement: sensor_aggr.. │                   │
│                    └────────────┬───────────────┘                   │
│                                 │                                   │
│                    ┌────────────▼───────────────┐                   │
│                    │     Grafana (:3000)         │                   │
│                    │  10 paneluri live           │                   │
│                    │  refresh: 5 secunde         │                   │
│                    └────────────────────────────┘                   │
└─────────────────────────────────────────────────────────────────────┘
```

## Structura proiectului

```
big-data-pipeline/
├── docker-compose.yml              # Orchestrare toate serviciile
├── .env                            # Variabile de configurare
│
├── simulator/
│   ├── Dockerfile
│   ├── requirements.txt
│   └── simulator.py                # Generator date senzori (8 orașe RO)
│
├── spark-processor/
│   ├── Dockerfile
│   └── processor.py                # Spark Structured Streaming job
│
└── grafana/
    └── provisioning/
        ├── datasources/
        │   └── influxdb.yml        # Conexiune auto InfluxDB
        └── dashboards/
            ├── dashboard.yml       # Loader dashboarduri
            └── sensor-dashboard.json  # 10 paneluri live
```

## Date simulate

| Câmp        | Descriere                        | Anomalie dacă         |
|-------------|----------------------------------|-----------------------|
| temperature | Temperatură (°C)                 | > 38°C sau < -5°C    |
| humidity    | Umiditate relativă (%)           | > 92%                 |
| co2_ppm     | Dioxid de carbon (ppm)           | > 1000 ppm            |
| pm2_5       | Particule fine (μg/m³)           | > 50 μg/m³            |
| pm10        | Particule (μg/m³)                | —                     |
| aqi         | Air Quality Index (simplificat)  | > 150                 |

**8 senzori**: București-Nord, București-Centru, București-Sud, Cluj-Napoca,
Timișoara, Iași, Constanța, Brașov.

Fiecare senzor are un profil climatic realist + ciclu zilnic sinusoidal +
anomalii injectate aleator (probabilitate ~1.5%) pentru testarea detecției.

## Prerequisite

- [Docker Desktop](https://www.docker.com/products/docker-desktop/) instalat și pornit
- Minim **6 GB RAM** alocate Docker
- Porturi libere: `3000`, `8080`, `8086`, `9092`

## Pornire (un singur comandă)

```bash
cd big-data-pipeline
docker-compose up --build
```

La prima rulare descarcă imaginile (~3–4 GB) și compilatorul Spark descarcă
conectorii Kafka (~150 MB). Durează ~3–5 minute.

## Acces servicii

| Serviciu     | URL                        | User / Parolă        |
|--------------|----------------------------|----------------------|
| **Grafana**  | http://localhost:3000      | admin / admin123     |
| **Kafka UI** | http://localhost:8080      | —                    |
| **InfluxDB** | http://localhost:8086      | admin / adminpassword123 |

Dashboardul Grafana se încarcă automat. Dacă nu apare, du-te la
*Dashboards → IoT Sensor Pipeline Dashboard*.

## Oprire

```bash
# Oprire (păstrează datele în volume)
docker-compose down

# Oprire + ștergere date
docker-compose down -v
```

## Împărțirea echipei

| Persoană | Componentă                          | Fișiere relevante                  |
|----------|-------------------------------------|------------------------------------|
| P1       | Ingestie & Simulare                 | `simulator/simulator.py`           |
| P2       | Procesare Spark Streaming           | `spark-processor/processor.py`     |
| P3       | Storage InfluxDB & schema date      | `docker-compose.yml` (influxdb)    |
| P4       | Vizualizare Grafana & documentație  | `grafana/`, `README.md`            |

## Flux de date detaliat

```
1. simulator.py publică un mesaj JSON la fiecare secundă per senzor
   → 8 mesaje/secundă în topicul Kafka "sensor-data"

2. Spark citește din Kafka cu startingOffsets=latest
   → parsează JSON cu schema strictă
   → adaugă coloana is_anomaly (boolean → int 0/1)
   → filtrează rândurile cu sensor_id null

3. Stream RAW (trigger: 5s)
   → foreachBatch → InfluxDB measurement "sensor_reading"
   → tag: sensor_id, location
   → fields: temperature, humidity, co2_ppm, pm2_5, pm10, aqi, is_anomaly

4. Stream AGREGAT (trigger: 30s, window: 1 min slide 30s, watermark: 2 min)
   → GROUP BY window + location
   → avg/max/min temperature, avg humidity, avg co2, avg aqi
   → sum(is_anomaly) = anomaly_count, count(*) = reading_count
   → foreachBatch → InfluxDB measurement "sensor_aggregation"
```

## Interogări Flux (InfluxDB) — exemple

```flux
// Temperatura medie ultimele 5 minute per locație
from(bucket: "sensors")
  |> range(start: -5m)
  |> filter(fn: (r) => r["_measurement"] == "sensor_reading")
  |> filter(fn: (r) => r["_field"] == "temperature")
  |> aggregateWindow(every: 30s, fn: mean, createEmpty: false)

// Total anomalii ultima oră
from(bucket: "sensors")
  |> range(start: -1h)
  |> filter(fn: (r) => r["_measurement"] == "sensor_reading")
  |> filter(fn: (r) => r["_field"] == "is_anomaly")
  |> group()
  |> sum()
```

## Troubleshooting

**Spark nu pornește / eroare la descărcat pachete**
```bash
docker-compose logs spark-processor
# Dacă e problemă de rețea, repornește:
docker-compose restart spark-processor
```

**Grafana nu afișează date**
- Verifică că InfluxDB e healthy: `docker-compose ps`
- Verifică că Spark scrie date: `docker-compose logs spark-processor`
- Setează time range în Grafana la "Last 15 minutes"

**Kafka UI nu vede mesaje**
- Mesajele vin la 1/secundă; topic-ul e creat automat la primul mesaj
- Verifică simulator: `docker-compose logs simulator`

## Tehnologii folosite

| Tehnologie        | Versiune | Rol                              |
|-------------------|----------|----------------------------------|
| Apache Kafka      | 7.5.0    | Message broker / event streaming |
| Apache Spark      | 3.5      | Structured Streaming processing  |
| InfluxDB          | 2.7      | Time-series database             |
| Grafana           | 10.2.0   | Visualization & dashboards       |
| Python            | 3.11     | Simulator & Spark driver         |
| Docker Compose    | 3.8      | Container orchestration          |
