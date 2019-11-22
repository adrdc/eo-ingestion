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
import io.pravega.avro.Status;
import io.pravega.client.ClientConfig;
import io.pravega.client.EventStreamClientFactory;
import io.pravega.client.SynchronizerClientFactory;
import io.pravega.client.admin.StreamManager;
import io.pravega.client.stream.*;
import lombok.Getter;
import lombok.val;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.io.DatumReader;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
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

public class PravegaSynchronizedWriter implements AutoCloseable {
    static final Logger log = LoggerFactory.getLogger(PravegaSynchronizedWriter.class);
    private Path path;

    @Getter
    private final String scope;
    @Getter
    private final String datastream;
    private final String syncstream;
    @Getter
    private final URI controller;
    private int startFileId;
    private SynchronizerClientFactory syncFactory;
    private ExactlyOnceIngestionSynchronizer synchronizer;
    private AtomicBoolean initialized = new AtomicBoolean(false);
    private AtomicInteger txnToFail = new AtomicInteger(0);
    private Status currentStatus;

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
            ClientConfig clientConfig = ClientConfig.builder()
                    .controllerURI(controller)
                    .build();

            try (StreamManager streamManager = StreamManager.create(controller)) {
                streamManager.createScope(scope);

                // Create sync stream
                StreamConfiguration syncStreamConfig = StreamConfiguration.builder()
                        .scalingPolicy(ScalingPolicy.fixed(1))
                        .build();
                streamManager.createStream(scope, syncstream, syncStreamConfig);

                // Create state synchronizer
                this.syncFactory = SynchronizerClientFactory.withScope(scope, clientConfig);
                this.synchronizer = ExactlyOnceIngestionSynchronizer.createNewSynchronizer(syncstream, syncFactory);

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
        this.currentStatus = this.synchronizer.getStatus();
        this.startFileId = this.currentStatus.getFileId();
        log.info("Upon recovery, current status is {}, {}", this.startFileId, this.currentStatus.getTxnId());
        if( this.currentStatus.getFileId() >= 0) {
            System.out.println("Running recovery, last file is " + this.currentStatus.getFileId());
            UUID txnId = UUID.fromString(this.currentStatus.getTxnId().toString());

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
                        this.startFileId = this.currentStatus.getFileId();

                        break;
                    case ABORTED:
                    case ABORTING:
                        this.startFileId = this.currentStatus.getFileId();

                        break;
                    case COMMITTED:
                    case COMMITTING:
                        this.startFileId = this.currentStatus.getFileId() + 1;

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
        log.info("Txn to fail: {}", this.txnToFail.get());
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
                                System.out.println("File " + f.getName());
                                System.out.println("\t Beginning transaction.");
                                log.info("Beginning transaction for file {}", f.getName());
                                Transaction<Sample> txn = writer.beginTxn();
                                // Add txn id and file name to state synchronizer
                                //this.synchronizer.updateStatus(new Status(fileId, txn.getTxnId()));
                                Status newStatus = Status.newBuilder()
                                        .setFileId(fileId)
                                        .setTxnId(txn.getTxnId().toString())
                                        .build();
                                if(!this.synchronizer.updateStatus(newStatus, this.currentStatus)) {
                                    throw new RuntimeException("Concurrent status update. Is there another writer running?");
                                };
                                this.currentStatus = newStatus;

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
                                    if (breakFlow(this.duringTxnFlag, txnCount, skip)) return;
                                } catch (IOException e) {
                                    throw new RuntimeException(e);
                                }
                                System.out.println("\t Committing transaction.");
                                log.info("Committing transaction for file {}", f.getName());
                                txn.commit();

                                // Set during tests to break the flow prematurely
                                if (breakFlow(this.afterCommitFlag, txnCount, skip)) return;
                            } catch (TxnFailedException e) {
                                throw new RuntimeException(e);
                            }
                        } else {
                            log.info("Skipping file {}, {}", fileId, this.startFileId);
                        }
                    }
            );
        }
    }

    @Override
    public void close() {
        this.syncFactory.close();
    }

    private boolean breakFlow(AtomicBoolean flag, AtomicInteger txnCount, AtomicBoolean skip) {
        if(flag.get()) {
            if (txnCount.get() == this.txnToFail.get()) {
                skip.set(true);

                return true;
            } else {
                txnCount.incrementAndGet();

                return false;
            }
        }

        return false;
    }

    public static void main (String[] args) {
        URI controller = URI.create("");
        Path path = Paths.get("");
        String streamName = "test-stream";

        Options options = new Options();

        options.addOption("p", true, "Path to data files");
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

        if (cmd.hasOption("p")) {
            path = Paths.get(cmd.getOptionValue("p"));
        }

        if (cmd.hasOption("s")) {
            streamName = cmd.getOptionValue("s");
        }

        if (cmd.hasOption("c")) {
            controller = URI.create(cmd.getOptionValue("c"));
            log.info("Controller URI: {}", controller);

            try( PravegaSynchronizedWriter writer =
                         new PravegaSynchronizedWriter(path, controller, streamName)) {
                writer.init();
                writer.run();
            }
        } else {
            log.error("No controller URL provided");
        }
    }
}
