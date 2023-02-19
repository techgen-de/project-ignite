
FROM jupyter/all-spark-notebook:latest

# Download the hadoop-azure.jar and azure-storage.jar files
RUN wget https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-azure/3.3.0/hadoop-azure-3.3.0.jar
RUN wget https://repo1.maven.org/maven2/com/microsoft/azure/azure-storage/7.0.1/azure-storage-7.0.1.jar

# Copy the jars to the Spark jars directory
USER root
RUN cp hadoop-azure-3.3.0.jar $SPARK_HOME/jars/
RUN cp azure-storage-7.0.1.jar $SPARK_HOME/jars/