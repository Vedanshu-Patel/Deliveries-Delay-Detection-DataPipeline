# Deliveries Delay Detection Data Pipeline

A real-time data pipeline for detecting and analyzing delivery delays by enriching delivery data with weather and traffic information using Apache Kafka and Apache Spark.

## Overview

This project implements a streaming data pipeline that:
- Ingests delivery data in real-time through Kafka
- Enriches delivery records with weather and traffic conditions
- Calculates delay metrics and driver fault percentages
- Outputs processed data to Parquet format for analytics

## Features

- **Real-time Streaming**: Uses Kafka for real-time data ingestion
- **Data Enrichment**: Joins delivery data with weather and traffic datasets
- **Delay Detection**: Identifies late deliveries and categorizes delays
- **Analytics**: Calculates delay scores, traffic scores, weather scores, and driver fault percentages
- **Scalable Processing**: Built on Apache Spark for distributed processing

## Architecture

```
CSV Data → Kafka Producer → Kafka Topic → Spark Streaming → Enriched Parquet Files
                                    ↓
                          Weather & Traffic Data
```

## Prerequisites

- **Python 3.7+**
- **Apache Kafka** (with Zookeeper)
- **Apache Spark 3.5.5+**
- **Java 8+** (required for Spark and Kafka)


## Data Structure

### Delivery Data (`data/deliveries_500k.csv`)
- `delivery_id`: Unique identifier for each delivery
- `city`: City where delivery occurs
- `pickup_time`: Timestamp when package was picked up
- `driver_id`: Identifier for the driver
- `expected_time`: Expected delivery timestamp
- `delivery_time`: Actual delivery timestamp

### Weather Data (`data/weather_data.csv`)
- `city`: City name
- `timestamp`: Hourly timestamp
- `temperature_c`: Temperature in Celsius
- `humidity_percent`: Humidity percentage
- `condition`: Weather condition (Clear, Clouds, Fog, Rain)

### Traffic Data (`data/traffic_data.csv`)
- `city`: City name
- `timestamp`: Hourly timestamp
- `traffic_level`: Traffic level (1-4, where 4 is heaviest)
- `avg_speed_kmph`: Average speed in km/h

## Usage

### 1. Start Kafka Services

**Start Zookeeper:**
```bash
bin\windows\zookeeper-server-start.bat config\zookeeper.properties
```

**Start Kafka Server:**
```bash
bin\windows\kafka-server-start.bat config\server.properties
```

**Create Kafka Topic:**
```bash
bin\windows\kafka-topics.bat --create --topic deliveries --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1
```

**Optional: Monitor Kafka messages:**
```bash
bin\windows\kafka-console-consumer.bat --topic deliveries --bootstrap-server localhost:9092 --from-beginning
```

### 2. Run the Producer

The producer reads delivery data from CSV and streams it to Kafka:

```bash
python scripts/producer.py
```

This script:
- Reads `data/deliveries_500k.csv`
- Sends each row as a JSON message to the `deliveries` Kafka topic
- Simulates real-time streaming with 1-second delays between messages

### 3. Run the Analytics Pipeline

The Spark streaming job processes Kafka messages and enriches them:

```bash
spark-submit --repositories https://repo.maven.apache.org/maven2 --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.5 --master local[*] scripts/analytics.py
```

**Note:** Before running, update the output paths in `analytics.py`:
- `output_path`: Directory for Parquet output files
- `checkpoint_path`: Directory for Spark checkpoint data

## Output Schema

The enriched output table contains the following columns:

| Column | Type | Description |
|--------|------|-------------|
| `delivery_id` | string | Unique delivery identifier |
| `city` | string | City name |
| `pickup_time` | timestamp | Package pickup timestamp |
| `delivery_time` | timestamp | Actual delivery timestamp |
| `expected_time` | timestamp | Expected delivery timestamp |
| `pickup_hour` | timestamp | Hour-truncated pickup time |
| `condition` | string | Weather condition |
| `traffic_level` | integer | Traffic level (1-4) |
| `is_late` | integer | 1 if delivery is late (>2 hours), 0 otherwise |
| `delay_minutes` | double | Delay in minutes (negative for early deliveries) |
| `delay_category` | string | Category: "Early", "On Time", "Minor Delay", "Major Delay" |
| `delay_score` | double | Normalized delay score (0.0-1.0) |
| `traffic_score` | double | Normalized traffic score (0.0-1.0) |
| `weather_score` | double | Normalized weather score (0.0-1.0) |
| `driver_fault_percentage` | double | Weighted fault percentage (delay: 50%, traffic: 30%, weather: 20%) |

### Delay Categories
- **Early**: Delivery completed before expected time
- **On Time**: Delay < 30 minutes
- **Minor Delay**: Delay between 30-120 minutes
- **Major Delay**: Delay ≥ 120 minutes

### Scoring Methodology
- **Delay Score**: Normalized from 0.0 (on time) to 1.0 (≥120 min delay)
- **Traffic Score**: Inverted traffic level (1.0 = light traffic, 0.0 = heavy traffic)
- **Weather Score**: 
  - Clear: 1.0
  - Clouds: 0.7
  - Fog: 0.4
  - Rain: 0.2
- **Driver Fault Percentage**: Weighted combination of all scores
