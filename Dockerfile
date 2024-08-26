# Use an official Spark image
FROM bitnami/spark:latest

# Set the working directory
WORKDIR /app

# Copy the PySpark job script to the container
COPY emr_spark_job.py .

# Run the PySpark job
ENTRYPOINT ["spark-submit", "emr_spark_job.py"]
