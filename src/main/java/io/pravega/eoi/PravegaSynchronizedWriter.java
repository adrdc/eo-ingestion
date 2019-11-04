package io.pravega.eoi;

import io.pravega.avro.Sample;
import io.pravega.client.ClientConfig;
import io.pravega.client.EventStreamClientFactory;
import io.pravega.client.SynchronizerClientFactory;
import io.pravega.client.admin.StreamManager;
import io.pravega.client.stream.*;
import lombok.Getter;
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
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

public class PravegaSynchronizedWriter {
    static final Logger log = LoggerFactory.getLogger(PravegaSynchronizedWriter.class);
    private Path path;

    private StreamManager streamManager;
    @Getter
    private final String scope;
    @Getter
    private final String datastream;
    private final String syncstream;
    @Getter
    private final URI controller;
    private int startFileId;
    private ExactlyOnceIngestionSynchronizer synchronizer;
    private AtomicBoolean initialized = new AtomicBoolean(false);
    private AtomicInteger txnToFail = new AtomicInteger(0);

    public PravegaSynchronizedWriter(Path path, URI controller, String streamName) {
        this.path = path;
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
            this.streamManager = StreamManager.create(controller);
            streamManager.createScope(scope);

            // Create sync stream
            StreamConfiguration syncStreamConfig = StreamConfiguration.builder()
                    .scalingPolicy(ScalingPolicy.fixed(1))
                    .build();
            streamManager.createStream(scope, syncstream, syncStreamConfig);

            ClientConfig clientConfig = ClientConfig.builder()
                    .controllerURI(controller)
                    .build();

            // Create state synchronizer
            SynchronizerClientFactory factory = SynchronizerClientFactory.withScope(scope, clientConfig);
            this.synchronizer = ExactlyOnceIngestionSynchronizer.createNewSynchronizer(syncstream, factory);

            // Create data stream
            StreamConfiguration dataStreamConfig = StreamConfiguration.builder()
                    .scalingPolicy(ScalingPolicy.fixed(10))
                    .build();
            streamManager.createStream(scope, datastream, dataStreamConfig);

            recover();
        }
        initialized.set(true);
    }

    private void recover() {
        this.synchronizer.update();
        Status current = this.synchronizer.getStatus();
        this.startFileId = current.getFileId();
        log.info("Upon recovery, current status is {}, {}", this.startFileId, current.getTxnId());
        if( current.getFileId() >= 0) {
            UUID txnId = current.getTxnId();

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
                switch(txn.checkStatus()) {
                    case OPEN:
                        txn.abort();
                        this.startFileId = current.getFileId();

                        break;
                    case ABORTED:
                    case ABORTING:
                    case COMMITTED:
                    case COMMITTING:
                        this.startFileId = current.getFileId() + 1;

                        break;
                };
            }
        }
    }

    enum FType {DuringTxn, AfterCommit};
    AtomicBoolean duringTxnFlag = new AtomicBoolean(false);
    AtomicBoolean afterCommitFlag = new AtomicBoolean(false);

    /**
     * For testing, inject failures so that we can validate approach.
     *
     * @param type
     */
    boolean induceFailure(FType type) {
        this.txnToFail.set((new Random()).nextInt(path.toFile().listFiles().length));
        switch (type) {
            case DuringTxn:
                if (!this.afterCommitFlag.get()) {
                    this.duringTxnFlag.set(true);

                    return true;
                }

                return false;
            case AfterCommit:
                if (!this.duringTxnFlag.get()) {
                    this.afterCommitFlag.set(true);

                    return true;
                }

                return false;
        }

        return false;
    }

    public void run() {
        ClientConfig clientConfig = ClientConfig.builder()
                .controllerURI(controller)
                .build();

        try (EventStreamClientFactory clientFactory = EventStreamClientFactory.withScope(scope, clientConfig);
             TransactionalEventStreamWriter<Sample> writer = clientFactory.
                     createTransactionalEventWriter(datastream,
                             new AvroSampleSerializer(),
                             EventWriterConfig.builder().build())) {
            final AtomicInteger txnCount = new AtomicInteger(0);
            final AtomicBoolean skip = new AtomicBoolean(false);
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
                        // Skip files that have already been committed
                        int fileId = Integer.parseInt(f.getName().split(".avro")[0]);
                        if( fileId >= this.startFileId && !skip.get()) {
                            try {
                                log.info("Beginning transaction for file {}", f.getName());
                                Transaction<Sample> txn = writer.beginTxn();
                                // Add txn id and file name to state synchronizer
                                this.synchronizer.updateStatus(new Status(fileId, txn.getTxnId()));


                                log.info("Added the following status: {}, {}", fileId, txn.getTxnId());
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

                                    // Set during tests to break the flow prematurely

                                    if (this.duringTxnFlag.get()) {
                                        if (txnCount.get() == this.txnToFail.get()) {
                                            log.info("Breaking the flow for file name {}", f.getName());
                                            skip.set(true);

                                            return;
                                        } else {
                                            txnCount.incrementAndGet();
                                        }
                                    }
                                } catch (IOException e) {
                                    throw new RuntimeException(e);
                                }
                                log.info("Committing transaction for file {}", f.getName());
                                txn.commit();

                                // Set during tests to break the flow prematurely
                                if (this.afterCommitFlag.get()) {
                                    if (txnCount.get() == this.txnToFail.get()) {
                                        return;
                                    } else {
                                        txnCount.incrementAndGet();
                                    }
                                }
                            } catch (TxnFailedException e) {
                                throw new RuntimeException(e);
                            }
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
        PravegaSynchronizedWriter writer = new PravegaSynchronizedWriter(path, controller, "test-stream");
        writer.run();
    }
}
