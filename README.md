<h2>Summary</h2>
This is Pyspark pipeline for consume message from kafka and insert processed record into Delta table.

In most data processing pipelines, the source is apache Kafka and  I needed to monitor the Kafka consumption status and
its lag in an external monitoring system. Therefore, I created this project and I used the following technologies:

- Spark Structured Streaming: scalable and fault-tolerant stream processing engine
- Kafka: message broker and source of the pipeline
- minio: distributed object storage for store processed date
- DeltaLake: an open-source storage framework that enables building 
a Lakehouse architecture with compute engines Like Spark
- prometheus, prometheus pushgateway and grafana for monitoring system

<h2>Faker</h2>
If Faker is enabled, in the background, fake data is generated at a defined rate(Faker_num_threads,Faker_sleep_s)

<h2>Metrics</h2>
The extraction of metrics is implemented in metrics.py, and it can be extended for other sources.
<h2>Test</h2>
I used the following Docker images to test the code:

- bitnami/kafka
- minio/minio
- prom/prometheus
- prom/pushgateway
- grafana/grafana
my spark version was 3.3.2 and delta 2.12