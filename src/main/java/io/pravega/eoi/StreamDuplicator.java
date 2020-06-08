package io.pravega.eoi;

import io.pravega.avro.Sample;
import io.pravega.avro.StreamPosition;
import io.pravega.client.ClientConfig;
import io.pravega.client.EventStreamClientFactory;
import io.pravega.client.SynchronizerClientFactory;
import io.pravega.client.admin.StreamManager;
import io.pravega.client.stream.Checkpoint;
import io.pravega.client.stream.EventWriterConfig;
import io.pravega.client.stream.ScalingPolicy;
import io.pravega.client.stream.StreamConfiguration;
import io.pravega.client.stream.Transaction;
import io.pravega.client.stream.TransactionalEventStreamWriter;
import io.pravega.eoi.synchronizer.ExactlyOnceStreamIngestionSynchronizer;
import lombok.Getter;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;
import java.nio.ByteBuffer;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

public class StreamDuplicator implements AutoCloseable {
    static final Logger log = LoggerFactory.getLogger(StreamDuplicator.class);

    @Getter
    private final String scope;
    @Getter
    private final String datastream;
    private final String syncstream;
    @Getter
    private final URI controller;
    private SynchronizerClientFactory syncFactory;
    private ExactlyOnceStreamIngestionSynchronizer synchronizer;
    private AtomicBoolean initialized = new AtomicBoolean(false);
    private AtomicInteger txnToFail = new AtomicInteger(0);
    private int cycle;
    private Checkpoint checkpoint;
    private StreamPosition currentPosition;


    public StreamDuplicator(URI controller, String streamName) {
        this.scope = "test-scope";
        this.datastream = streamName;
        this.syncstream = "sync-" + streamName;
        this.controller = controller;
    }

    private boolean isInitialized() {
        return false;
    }

    public void init() {
        if (!isInitialized()) {
            ClientConfig clientConfig = ClientConfig.builder()
                    .controllerURI(controller)
                    .build();

            try (StreamManager streamManager = StreamManager.create(controller)) {
                streamManager.createScope(scope);

                // Create sync stream
                StreamConfiguration syncStreamConfig = StreamConfiguration.builder()
                        .scalingPolicy(ScalingPolicy.fixed(10))
                        .build();
                streamManager.createStream(scope, syncstream, syncStreamConfig);

                // Create state synchronizer
                this.syncFactory = SynchronizerClientFactory.withScope(scope, clientConfig);
                this.synchronizer = ExactlyOnceStreamIngestionSynchronizer.createNewSynchronizer(syncstream, syncFactory);

                // Create data stream
                StreamConfiguration dataStreamConfig = StreamConfiguration.builder()
                        .scalingPolicy(ScalingPolicy.fixed(10))
                        .build();
                streamManager.createStream(scope, datastream, dataStreamConfig);

                recover();
            }
        }
        initialized.set(true);
    }

    private void recover() {
        this.synchronizer.update();
        this.currentPosition = this.synchronizer.getStreamPosition();
        if( this.currentPosition.getCycle() > 0) {
            log.info("Running recovery, last cycle is " + this.currentPosition.getCycle());
            UUID txnId = UUID.fromString(this.currentPosition.getTxnId().toString());

            ClientConfig clientConfig = ClientConfig.builder()
                    .controllerURI(controller)
                    .build();

            log.info("Creating writer for stream {}", datastream);
            try (EventStreamClientFactory clientFactory = EventStreamClientFactory.withScope(scope, clientConfig);
                 TransactionalEventStreamWriter<Sample> writer = clientFactory.
                         createTransactionalEventWriter(datastream,
                                 new AvroSampleSerializer(),
                                 EventWriterConfig.builder().build())) {
                Transaction<Sample> txn = writer.getTxn(txnId);
                Transaction.Status status = txn.checkStatus();

                switch(status) {
                    case OPEN:
                        txn.abort();
                        this.cycle = this.currentPosition.getCycle();

                        break;
                    case ABORTED:
                    case ABORTING:
                        this.cycle = this.currentPosition.getCycle();

                        break;
                    case COMMITTED:
                    case COMMITTING:
                        this.cycle = this.currentPosition.getCycle() + 1;

                        break;
                }

                this.checkpoint = Checkpoint.fromBytes(this.currentPosition.getCheckpoint());
            }
        }

    }

    public void run() {
        // Create reader group from last checkpoint or beginning of the stream

        // Read from stream nd write to txn

        //

    }

    @Override
    public void close() {
        this.syncFactory.close();
    }

    /**
     * The main method to trigger the writer. Two command line options
     * are necessary to start it:
     *
     * 1- The directory containing the files
     * 2- The URI of the controller
     *
     * The files can be generated with the FileSampleGenerator in this project.
     * The sample generator produces a number of avro files.
     * */

    public static void main (String[] args) {
        URI controller = URI.create("");
        String streamName = "test-stream";


        Options options = new Options();

        options.addOption("c", true, "Controller URI");
        options.addOption("s", true, "Stream name" );

        CommandLineParser parser = new DefaultParser();
        CommandLine cmd;

        try {
            cmd = parser.parse(options, args);
        } catch (ParseException e) {
            log.error("Error while parsing options", e);
            return;
        }

        if (cmd.hasOption("s")) {
            streamName = cmd.getOptionValue("s");
        }

        if (cmd.hasOption("c")) {
            controller = URI.create(cmd.getOptionValue("c"));
            log.info("Controller URI: {}", controller);

            try( StreamDuplicator duplicator =
                         new StreamDuplicator(controller, streamName)) {
                duplicator.init();
                duplicator.run();
            }
        } else {
            log.error("No controller URL provided");
        }
    }

}
