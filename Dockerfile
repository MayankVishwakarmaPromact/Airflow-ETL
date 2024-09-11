# Step 1: Use the official Airflow image as the base image
FROM apache/airflow:2.10.1

# Step 2: Switch to root user to install system-level packages
USER root

# Step 3: Install the default JDK and other necessary packages
RUN apt-get update && \
    apt-get install -y default-jdk wget unzip && \
    apt-get clean

# Step 4: Set JAVA_HOME environment variable
ENV JAVA_HOME=/usr/lib/jvm/default-java
ENV PATH=$JAVA_HOME/bin:$PATH

# Step 5: Download and add the PostgreSQL JDBC driver
RUN mkdir -p /opt/jars
RUN wget -O /opt/jars/postgresql-42.3.6.jar https://search.maven.org/remotecontent?filepath=org/postgresql/postgresql/42.3.6/postgresql-42.3.6.jar

# Step 6: Switch back to the airflow user
USER airflow

# Step 7: Copy the requirements.txt to the container
COPY requirements.txt /requirements.txt

# Step 8: Install Python dependencies from requirements.txt
RUN pip install --no-cache-dir -r /requirements.txt

# Step 9: Set Spark configuration for JDBC drivers
ENV SPARK_SUBMIT_OPTIONS="--jars /opt/jars/postgresql-42.3.6.jar"

# Optional: Copy your Airflow DAGs or any additional files
# COPY dags/ /opt/airflow/dags/
