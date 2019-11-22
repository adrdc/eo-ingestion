# eo-ingestion
This sample code shows how to ingest data into a Pravega stream in an exactly-once manner using transactions and the state synchronizer, features of Pravega. The main class to look for is `PravegaSynchronizedWriter`.

To run this sample, follow these steps:

0- Generate sample files

```
	mvn exec:java -Dexec.mainClass="io.pravega.data.FileSampleGenerator" -Dexec.args="-p=/tmp/eo-ingestion-files -f=50 -r=1000" -Dlog4j.configuration=file:conf/log4j.properties
```

1- Start Pravega Standalone
```
	./gradlew :standalone:startStandalone
```

2- Run Pravega Writer
```
	mvn exec:java -Dexec.mainClass="io.pravega.eoi.PravegaSynchronizedWriter" -Dexec.args="-p=/tmp/eo-ingestion-files/ -c=tcp://localhost:9090 -s=teststream" -Dlog4j.configuration=file:conf/log4j.properties
```

3- Maybe break step 2 in the middle and restart step 2

4- Use `simplereader` to validate that we have all messages and no duplicate. `simplereader` is a no-op Flink job. Given that we have 50 files and 1000 events per file, there should be 50,000 events in the stream
```
	bin/flink run /home/fpj/code/simplereader/target/simplereader-1.0-SNAPSHOT.jar --stream teststream
```

See `https://github.com/fpj/simplereader`.
