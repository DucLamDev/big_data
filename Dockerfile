# Sử dụng Python 3.9 làm base image
FROM python:3.9-slim

# Thiết lập các biến môi trường
ENV JAVA_HOME /usr/lib/jvm/java-17-openjdk-amd64
ENV SPARK_HOME /opt/spark
ENV PYTHONPATH $SPARK_HOME/python:$SPARK_HOME/python/lib/py4j-0.10.9.7-src.zip
ENV PATH $PATH:$SPARK_HOME/bin

# Cài đặt Java, Spark và các dependencies
RUN apt-get update && \
  apt-get install -y --no-install-recommends \
  openjdk-17-jdk-headless \
  wget \
  && rm -rf /var/lib/apt/lists/*

# Tải và cài đặt Spark 3.4.4
RUN wget https://dlcdn.apache.org/spark/spark-3.4.4/spark-3.4.4-bin-hadoop3.tgz && \
  tar -xzf spark-3.4.4-bin-hadoop3.tgz && \
  mv spark-3.4.4-bin-hadoop3 /opt/spark && \
  rm spark-3.4.4-bin-hadoop3.tgz
RUN pip install --upgrade pip

# Thiết lập thư mục làm việc trong container
WORKDIR /app

# Copy toàn bộ mã nguồn vào container
COPY . /app

# Cài đặt các thư viện Python từ requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

# Cài đặt Delta Lake JAR
RUN wget https://repo1.maven.org/maven2/io/delta/delta-core_2.12/2.4.0/delta-core_2.12-2.4.0.jar -P $SPARK_HOME/jars/ && \
  wget https://repo1.maven.org/maven2/io/delta/delta-storage/2.4.0/delta-storage-2.4.0.jar -P $SPARK_HOME/jars/

# Mở port 5000
EXPOSE 5000

# Chạy ứng dụng
CMD ["python", "app/app.py"]

