# manage.ps1
param (
    [Parameter(Mandatory=$true)]
    [ValidateSet("up", "down", "destroy", "ps", "logs", "health", "load-raw", "produce-events", "spark-batch", "spark-streaming", "dbt-run", "dq-check", "test")]
    [string]$Action
)

switch ($Action) {
    "up" {
        docker compose up -d
    }
    "down" {
        docker compose down
    }
    "destroy" {
        docker compose down -v
    }
    "ps" {
        docker compose ps
    }
    "logs" {
        docker compose logs -f --tail=100
    }
    "health" {
        Write-Host "--- Memeriksa Kesehatan Seluruh Layanan ---" -ForegroundColor Cyan
        Write-Host "1. MinIO API        : " -NoNewline; try { (Invoke-WebRequest -Uri "http://localhost:9000/minio/health/live" -UseBasicParsing).StatusCode } catch { "DOWN" }
        Write-Host "2. Airflow Web      : " -NoNewline; try { (Invoke-WebRequest -Uri "http://localhost:8080/health" -UseBasicParsing).StatusCode } catch { "DOWN" }
        Write-Host "3. Spark Master UI  : " -NoNewline; try { (Invoke-WebRequest -Uri "http://localhost:8081" -UseBasicParsing).StatusCode } catch { "DOWN" }
        Write-Host "4. Postgres DWH     : " -NoNewline; docker exec postgres-dwh pg_isready -U dwh_user -d warehouse
        Write-Host "5. Metabase BI      : " -NoNewline; try { (Invoke-WebRequest -Uri "http://localhost:3000/api/health" -UseBasicParsing).StatusCode } catch { "DOWN" }
        Write-Host "6. Prometheus       : " -NoNewline; try { (Invoke-WebRequest -Uri "http://localhost:9090/-/healthy" -UseBasicParsing).StatusCode } catch { "DOWN" }
        Write-Host "7. Grafana UI       : " -NoNewline; try { (Invoke-WebRequest -Uri "http://localhost:3001/api/health" -UseBasicParsing).StatusCode } catch { "DOWN" }
    }
    "load-raw" {
        docker exec -it airflow-scheduler python3 /opt/ingestion/batch_loader.py
    }
    "produce-events" {
        docker exec -it airflow-scheduler python3 /opt/ingestion/clickstream_producer.py
    }
    "spark-batch" {
        docker exec -u 0 spark-master spark-submit --master spark://spark-master:7077 /opt/spark_jobs/batch/bronze_to_silver.py
        docker exec -u 0 spark-master spark-submit --master spark://spark-master:7077 /opt/spark_jobs/batch/silver_to_gold.py
        docker exec -u 0 spark-master spark-submit --master spark://spark-master:7077 --packages org.postgresql:postgresql:42.7.3 /opt/spark_jobs/batch/load_to_warehouse.py
    }
    "spark-streaming" {
        docker exec -u 0 spark-master spark-submit --master spark://spark-master:7077 --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1 /opt/spark_jobs/streaming/kafka_to_bronze.py
    }
    "dbt-run" {
        docker exec -it airflow-scheduler bash -c "cd /opt/dbt/ecommerce_dwh && dbt run --profiles-dir . && dbt test --profiles-dir ."
    }
    "dq-check" {
        docker exec -it airflow-scheduler bash -c "cd /opt/great_expectations && great_expectations checkpoint run bronze_orders_checkpoint && great_expectations checkpoint run silver_orders_checkpoint"
    }
    "test" {
        docker exec -e PYTHONPATH=/opt/ingestion:/opt airflow-scheduler python -m pytest /opt/tests -v
        docker exec -u 0 spark-master pytest /opt/spark_jobs/tests -v
    }
}
