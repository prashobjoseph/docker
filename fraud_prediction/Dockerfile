# Use a base image with Java and Python
FROM openjdk:8-jre-slim-buster

# Install required dependencies
RUN apt-get update && apt-get install -y \
    python3 \
    python3-pip \
    openssh-client \
    openssh-server \
    wget \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Install PySpark, Jupyter, and dependencies
RUN pip3 install \
    pyspark==3.2.1 \
    jupyter \
    findspark \
    numpy \
    pandas

# Set environment variables for Spark
ENV SPARK_HOME=/opt/spark
ENV PATH=$PATH:$SPARK_HOME/bin:$SPARK_HOME/sbin
ENV PYSPARK_DRIVER_PYTHON=jupyter
ENV PYSPARK_DRIVER_PYTHON_OPTS='notebook --ip=0.0.0.0 --port=8889 --allow-root --NotebookApp.token="" --NotebookApp.password=""'

# Copy Spark from external source (You can also download Spark manually)
RUN wget https://downloads.apache.org/spark/spark-3.2.1/spark-3.2.1-bin-hadoop3.2.tgz -O /tmp/spark.tgz && \
    tar -xzf /tmp/spark.tgz -C /opt/ && \
    mv /opt/spark-3.2.1-bin-hadoop3.2 /opt/spark && \
    rm /tmp/spark.tgz

# SSH Setup (if needed)
RUN mkdir /var/run/sshd && chmod 0755 /var/run/sshd
RUN echo "root:root" | chpasswd

# Expose Ports for Jupyter and Spark
EXPOSE 8889 4040 22

# Create working directory
WORKDIR /app

# Copy your PySpark script (if any)
COPY fraud_prediction.ipynb /app/fraud_prediction.ipynb

# Start SSH and PySpark Jupyter Notebook
CMD service ssh start && pyspark --master yarn
