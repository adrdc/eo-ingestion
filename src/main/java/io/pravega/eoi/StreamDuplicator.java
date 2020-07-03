/*
 * Copyright 2019 Flavio Junqueira
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.pravega.eoi;

import io.pravega.avro.Sample;
import io.pravega.avro.StreamPosition;
import io.pravega.client.ClientConfig;
import io.pravega.client.EventStreamClientFactory;
import io.pravega.client.SynchronizerClientFactory;
import io.pravega.client.admin.ReaderGroupManager;
import io.pravega.client.admin.StreamManager;
import io.pravega.client.stream.Checkpoint;
import io.pravega.client.stream.EventRead;
import io.pravega.client.stream.EventStreamReader;
import io.pravega.client.stream.EventWriterConfig;
import io.pravega.client.stream.ReaderConfig;
import io.pravega.client.stream.ReaderGroup;
import io.pravega.client.stream.ReaderGroupConfig;
import io.pravega.client.stream.ReaderGroupConfig.ReaderGroupConfigBuilder;
import io.pravega.client.stream.ScalingPolicy;
import io.pravega.client.stream.Stream;
import io.pravega.client.stream.StreamConfiguration;
import io.pravega.client.stream.Transaction;
import io.pravega.client.stream.TransactionalEventStreamWriter;
import io.pravega.client.stream.TxnFailedException;
import io.pravega.common.concurrent.ExecutorServiceHelpers;
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
import java.time.Duration;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

public class StreamDuplicator implements AutoCloseable {
    static final Logger log = LoggerFactory.getLogger(StreamDuplicator.class);

    @Getter
    private final String scope;
    @Getter
    private final String inputdatastream;
    @Getter
    private final String outputdatastream;
    private final String syncstream;
    @Getter
    private final URI controller;
    private final int checkpointFrequency;

    private ReaderGroupManager rgManager;
    private SynchronizerClientFactory syncFactory;
    private ExactlyOnceStreamIngestionSynchronizer synchronizer;
    private AtomicBoolean initialized = new AtomicBoolean(false);
    private AtomicInteger txnToFail = new AtomicInteger(0);
    private int cycle;
    private Checkpoint checkpoint;
    private StreamPosition currentPosition;


    public StreamDuplicator(URI controller,
                            String inputStreamName,
                            String outputStreamName) {
        this.scope = "test-scope";
        this.inputdatastream = inputStreamName;
        this.outputdatastream = outputStreamName;
        this.syncstream = "sync-" + this.outputdatastream;
        this.controller = controller;
        this.checkpoint = null;
        this.checkpointFrequency = 1000;
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

                // Create data streams
                StreamConfiguration dataStreamConfig = StreamConfiguration.builder()
                        .scalingPolicy(ScalingPolicy.fixed(10))
                        .build();
                streamManager.createStream(scope, outputdatastream, dataStreamConfig);

                // Create reader group manager
                this.rgManager =  ReaderGroupManager.withScope("scope", controller);

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

            log.info("Creating writer for stream {}", outputdatastream);
            try (EventStreamClientFactory clientFactory = EventStreamClientFactory.withScope(scope, clientConfig);
                 TransactionalEventStreamWriter<Sample> writer = clientFactory.
                         createTransactionalEventWriter(outputdatastream,
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
        ScheduledExecutorService executor = ExecutorServiceHelpers.newScheduledThreadPool(1, "executor");
        // Create reader group from last checkpoint or beginning of the stream
        ReaderGroupConfigBuilder rgConfig = ReaderGroupConfig.builder();
        if (this.checkpoint != null) {
            rgConfig.startFromCheckpoint(this.checkpoint);
        }

        rgConfig.disableAutomaticCheckpoints()
                .startFromCheckpoint(this.checkpoint)
                .stream(Stream.of(scope, inputdatastream));

        this.rgManager.createReaderGroup("duplicator", rgConfig.build());

        ClientConfig clientConfig = ClientConfig.builder()
                .controllerURI(controller)
                .build();

        ReaderConfig readerConfig = ReaderConfig.builder().disableTimeWindows(true).build();

        try (// Sets up readers and writer
             EventStreamClientFactory clientFactory = EventStreamClientFactory.withScope(scope, clientConfig);
             EventStreamReader reader = clientFactory.createReader("reader",
                     "duplicator",
                     new AvroSampleSerializer(),
                     readerConfig);
             TransactionalEventStreamWriter<Sample> writer = clientFactory.
                     createTransactionalEventWriter(outputdatastream,
                             new AvroSampleSerializer(),
                             EventWriterConfig.builder().build());
             ReaderGroup readerGroup = this.rgManager.getReaderGroup("duplicator");
        ) {
            long counter = 0;
            Future<Checkpoint> ckFuture = null;
            Transaction<Sample> txn = writer.beginTxn();
            StreamPosition newPosition = StreamPosition.newBuilder()
                    .setCycle(this.cycle >= 0 ? this.cycle : 0)
                    .setCheckpoint(this.checkpoint == null ? ByteBuffer.allocate(0) : this.checkpoint.toBytes())
                    .setTxnId(txn.getTxnId().toString())
                    .build();

            // Update the synchronizer state either with a new position, which can be the first
            // transaction that we create or a subsequent new transaction post failover.
            synchronizer.updateStatus(newPosition, this.currentPosition);

            while (true) {
                // Read one event from input stream
                EventRead read = reader.readNextEvent(Duration.ofMinutes(1).toMillis());

                // If it is not a checkpoint event, then append the event to the
                // output stream.
                if (!read.isCheckpoint()) {
                    Sample sample = ((Sample) read.getEvent());
                    txn.writeEvent(sample.getId().toString(), sample);

                    // Time to execute another checkpoint
                    if (counter++ >= this.checkpointFrequency) {
                        readerGroup.initiateCheckpoint("dupl-checkpoint", executor);
                        counter = 0;
                    }
                } else {
                    this.checkpoint = ckFuture.get();
                    // Do we need to validate somehow that the commit has gone through?
                    txn.commit();
                    txn = writer.beginTxn();
                    this.cycle++;

                    // Create a new stream position to update the synchronizer state
                    newPosition = StreamPosition.newBuilder()
                            .setCycle(this.cycle)
                            .setCheckpoint(this.checkpoint.toBytes())
                            .setTxnId(txn.getTxnId().toString())
                            .build();

                    // Update the status
                    synchronizer.updateStatus(newPosition, this.currentPosition);
                    this.currentPosition = newPosition;
                }
            }
        } catch (TxnFailedException e) {
            throw new RuntimeException(e);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(e);
        } catch (ExecutionException e) {
            throw new RuntimeException(e);
        }
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
        URI controller;
        String inputStreamName = "input-test-stream";
        String outputStreamName = "output-test-stream";


        Options options = new Options();

        options.addOption("c", true, "Controller URI");
        options.addOption("i", true, "Input stream name" );
        options.addOption("o", true, "Output stream name" );

        CommandLineParser parser = new DefaultParser();
        CommandLine cmd;

        try {
            cmd = parser.parse(options, args);
        } catch (ParseException e) {
            log.error("Error while parsing options", e);
            return;
        }

        if (cmd.hasOption("i")) {
            inputStreamName = cmd.getOptionValue("i");
        }

        if (cmd.hasOption("o")) {
            outputStreamName = cmd.getOptionValue("o");
        }

        if (cmd.hasOption("c")) {
            controller = URI.create(cmd.getOptionValue("c"));
            log.info("Controller URI: {}", controller);

            try( StreamDuplicator duplicator =
                         new StreamDuplicator(controller,
                                 inputStreamName,
                                 outputStreamName)) {
                duplicator.init();
                duplicator.run();
            }
        } else {
            log.error("No controller URL provided");
        }
    }

}
