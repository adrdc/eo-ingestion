package io.pravega.eoi;

import io.pravega.avro.Sample;
import io.pravega.client.ClientConfig;
import io.pravega.client.EventStreamClientFactory;
import io.pravega.client.SynchronizerClientFactory;
import io.pravega.client.admin.StreamManager;
import io.pravega.client.stream.*;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.io.DatumReader;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.commons.cli.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;

public class PravegaSynchronizedWriter {
    static final Logger log = LoggerFactory.getLogger(PravegaSynchronizedWriter.class);
    private Path path;
    private final String scope;
    private final String datastream;
    private final String syncstream;
    private final URI controller;
    private ExactlyOnceIngestionSynchronizer synchronizer;

    public PravegaSynchronizedWriter(Path path, URI controller) {
        this.path = path;
        this.scope = "test-scope";
        this.datastream = "test-stream";
        this.syncstream = "sync-stream";
        this.controller = controller;
    }

    private boolean isInitialized() {
        return false;
    }

    public void init() {
        if (!isInitialized()) {
            StreamManager streamManager = StreamManager.create(controller);
            streamManager.createScope(scope);

            StreamConfiguration streamConfig = StreamConfiguration.builder()
                    .scalingPolicy(ScalingPolicy.fixed(10))
                    .build();
            streamManager.createStream(scope, syncstream, streamConfig);

            ClientConfig clientConfig = ClientConfig.builder()
                    .controllerURI(controller)
                    .build();
            SynchronizerClientFactory factory = SynchronizerClientFactory.withScope(scope, clientConfig);
            this.synchronizer = ExactlyOnceIngestionSynchronizer
                    .createNewSynchronizer(Stream.of(scope, syncstream).getScopedName(), factory);
        }
    }
    public void run() {

        StreamManager streamManager = StreamManager.create(controller);
        streamManager.createScope(scope);

        StreamConfiguration streamConfig = StreamConfiguration.builder()
                .scalingPolicy(ScalingPolicy.fixed(10))
                .build();
        streamManager.createStream(scope, datastream, streamConfig);

        ClientConfig clientConfig = ClientConfig.builder()
                .controllerURI(controller)
                .build();

        try (EventStreamClientFactory clientFactory = EventStreamClientFactory.withScope(scope, clientConfig);
             TransactionalEventStreamWriter<Sample> writer = clientFactory.
                     createTransactionalEventWriter(datastream,
                             new AvroSampleSerializer(),
                             EventWriterConfig.builder().build())) {

            Arrays.stream(path.toFile().listFiles()).sorted( (f1, f2) -> {
                try {
                    int i1 = Integer.parseInt(f1.getName().split(".avro")[0]);
                    int i2 = Integer.parseInt(f2.getName().split(".avro")[0]);
                    return i1 - i2;
                } catch(NumberFormatException e) {
                    throw new AssertionError(e);
                }
            }).forEach(
                    f -> {
                        try {
                            Transaction<Sample> txn = writer.beginTxn();
                            // Add txn id and file name to state synchronizer
                            try {

                                DatumReader<Sample> userDatumReader = new SpecificDatumReader<>(Sample.class);
                                DataFileReader<Sample> dataFileReader = new DataFileReader<>(f, userDatumReader);
                                dataFileReader.forEach(sample -> {
                                    try {
                                        txn.writeEvent(sample.getId().toString(), sample);
                                    } catch (TxnFailedException e) {
                                        throw new RuntimeException(e);
                                    }
                                });
                            } catch (IOException e) {
                                throw new RuntimeException(e);
                            }
                            txn.commit();
                        } catch (TxnFailedException e) {
                            throw new RuntimeException(e);
                        }
                    }
            );
        }
    }

    public static void main (String[] args) {
        URI controller = URI.create("");
        Path path = Paths.get("");

        Options options = new Options();

        options.addOption("p", true, "Path to data files");
        options.addOption("c", true, "Controller URI");

        CommandLineParser parser = new DefaultParser();
        CommandLine cmd;

        try {
            cmd = parser.parse(options, args);
        } catch (ParseException e) {
            log.error("Error while parsing options", e);
            return;
        }

        if (cmd.hasOption("p")) {
            path = Paths.get(cmd.getOptionValue("p"));
        }

        if (cmd.hasOption("c")) {
            controller = URI.create(cmd.getOptionValue("c"));
        }
        PravegaWriter writer = new PravegaWriter(path, controller);
        writer.run();
    }
}
