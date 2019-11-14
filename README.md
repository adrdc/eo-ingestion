# eo-ingestion
Sample exactly-once ingestion

To create sample files:

```
mvn -X exec:java -Dexec.mainClass="io.pravega.data.FileSampleGenerator" -Dexec.args="-p=/tmp/eo-ingestion-files -f=50 -r=1000" -Dlog4j.configuration=file:conf/log4j.properties

```

To run PravegaSynchronizewdWriter:

```
mvn -X exec:java -Dexec.mainClass="io.pravega.eoi.PravegaSynchronizedWriter" -Dexec.args="-p=/tmp/eo-ingestion-files/ -c=tcp://localhost:9090" -Dlog4j.configuration=file:conf/log4j.properties
```
