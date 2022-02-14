# Dockerfile References: https://docs.docker.com/engine/reference/builder/
FROM gcr.io/datamechanics/spark:platform-3.1-dm14

ENV PYSPARK_MAJOR_PYTHON_VERSION=3
WORKDIR /opt/covid/

RUN wget  https://jdbc.postgresql.org/download/postgresql-42.2.5.jar
RUN mv postgresql-42.2.5.jar /opt/spark/jars

RUN wget https://covid.ourworldindata.org/data/owid-covid-data.csv

COPY requirements.txt .
COPY . .
RUN pip3 install -r requirements.txt

CMD ["sh", "-c", "python -m reporting"]
#CMD ["sh", "-c", "sleep 100000"]