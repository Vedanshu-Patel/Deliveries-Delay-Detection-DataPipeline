Final enriched table schema
____________________________

root
 |-- delivery_id: string
 |-- city: string
 |-- pickup_time: timestamp
 |-- delivery_time: timestamp
 |-- expected_time: timestamp
 |-- pickup_hour: timestamp
 |-- condition: string
 |-- traffic_level: integer
 |-- is_late: integer (1 or 0)
 |-- delay_minutes: double
 |-- delay_category: string (Early, On Time, etc.)
 |-- delay_score: double
 |-- traffic score: double
 |-- weather_score: double
 |-- driver_fault_percentage: double

Kafka
_________

bin\windows\zookeeper-server-start.bat config\zookeeper.properties

bin\windows\kafka-server-start.bat config\server.properties

bin\windows\kafka-topics.bat --create --topic deliveries --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1

bin\windows\kafka-console-consumer.bat --topic deliveries --bootstrap-server localhost:9092 --from-beginning


Spark Submit
_____________
run producer.py

spark-submit --repositories https://repo.maven.apache.org/maven2 --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.5 --master local[*] "file path to analytics.py"
