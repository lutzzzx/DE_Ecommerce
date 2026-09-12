.PHONY: up down destroy logs ps health load-raw produce-events spark-batch spark-streaming dbt-run dq-check test

# Menyalakan seluruh service container
up:
	docker compose up -d

# Menghentikan seluruh container (data volume tetap aman)
down:
	docker compose down

# Menghentikan dan menghapus semua volume (RESET TOTAL - HATI-HATI)
destroy:
	docker compose down -v

# Memantau log gabungan container
logs:
	docker compose logs -f --tail=100

# Memeriksa status kesehatan container
ps:
	docker compose ps

# Memuat data batch mentah CSV ke MinIO Bronze
load-raw:
	docker exec -it airflow-scheduler python3 /opt/ingestion/batch_loader.py

# Memulai streaming producer Kafka (Ctrl+C untuk berhenti)
produce-events:
	docker exec -it airflow-scheduler python3 /opt/ingestion/clickstream_producer.py

# Menjalankan transformasi batch Spark (Bronze -> Silver -> Gold -> Warehouse)
spark-batch:
	docker exec -u 0 spark-master spark-submit --master spark://spark-master:7077 /opt/spark_jobs/batch/bronze_to_silver.py
	docker exec -u 0 spark-master spark-submit --master spark://spark-master:7077 /opt/spark_jobs/batch/silver_to_gold.py
	docker exec -u 0 spark-master spark-submit --master spark://spark-master:7077 --packages org.postgresql:postgresql:42.7.3 /opt/spark_jobs/batch/load_to_warehouse.py

# Menjalankan consumer streaming Spark (Kafka -> MinIO Bronze)
spark-streaming:
	docker exec -u 0 spark-master spark-submit --master spark://spark-master:7077 --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1 /opt/spark_jobs/streaming/kafka_to_bronze.py

# Menjalankan pemodelan dan pengujian dbt
dbt-run:
	docker exec -it airflow-scheduler bash -c "cd /opt/dbt/ecommerce_dwh && dbt run --profiles-dir . && dbt test --profiles-dir ."

# Menjalankan checkpoint Great Expectations
dq-check:
	docker exec -it airflow-scheduler bash -c "cd /opt/great_expectations && great_expectations checkpoint run bronze_orders_checkpoint && great_expectations checkpoint run silver_orders_checkpoint"

# Menjalankan unit test & test integritas
test:
	docker exec -e PYTHONPATH=/opt/ingestion:/opt airflow-scheduler python -m pytest /opt/tests -v
	docker exec -u 0 spark-master pytest /opt/spark_jobs/tests -v
