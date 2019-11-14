package io.pravega.flink;

import io.pravega.avro.Sample;
import io.pravega.client.stream.Stream;
import io.pravega.connectors.flink.FlinkPravegaReader;
import io.pravega.connectors.flink.PravegaConfig;
import io.pravega.connectors.flink.serialization.PravegaDeserializationSchema;
import io.pravega.eoi.AvroSampleSerializer;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SimpleReader {

    // Logger initialization
    private static final Logger LOG = LoggerFactory.getLogger(SimpleReader.class);

    // The application reads data from specified Pravega stream

    // Application parameters
    //   stream - default test-scope/eo-ingestion
    //   controller - default tcp://127.0.0.1:9090

    public static void main(String[] args) throws Exception {
        LOG.info("Starting Simple Stream Reader...");

        // initialize the parameter utility tool in order to retrieve input parameters
        ParameterTool params = ParameterTool.fromArgs(args);
        PravegaConfig pravegaConfig = PravegaConfig
                .fromParams(params)
                .withDefaultScope(Constants.DEFAULT_SCOPE);

        // initialize the Flink execution environment
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // create the Pravega source to read a stream of text
        FlinkPravegaReader<Sample> source = FlinkPravegaReader.<Sample>builder()
                .withPravegaConfig(pravegaConfig)
                .forStream(Stream.of("test-scope", "test-stream"))
                .withDeserializationSchema(new PravegaDeserializationSchema<>(Sample.class, new AvroSampleSerializer()))
                .build();

        // simply read events
        DataStream dataStream = env.addSource(source).name("Pravega Stream");

        // create an output sink to print to stdout for verification
        dataStream.print();

        // execute within the Flink environment
        env.execute("SimpleReader");

        LOG.info("Ending SimpleReader...");
    }
}
