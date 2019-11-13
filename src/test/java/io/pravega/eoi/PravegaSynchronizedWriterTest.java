package io.pravega.eoi;

import io.pravega.avro.Sample;
import io.pravega.client.ClientConfig;
import io.pravega.client.EventStreamClientFactory;
import io.pravega.client.admin.ReaderGroupManager;
import io.pravega.client.stream.EventRead;
import io.pravega.client.stream.EventStreamReader;
import io.pravega.client.stream.ReaderConfig;
import io.pravega.client.stream.ReaderGroupConfig;
import io.pravega.client.stream.Stream;
import io.pravega.data.FileSampleGenerator;
import io.pravega.utils.SetupUtils;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.atomic.AtomicReference;

public class PravegaSynchronizedWriterTest {
    static final Logger log = LoggerFactory.getLogger(PravegaSynchronizedWriterTest.class);

    protected static final AtomicReference<SetupUtils> SETUP_UTILS = new AtomicReference();

    @BeforeAll
    public static void setup() throws Exception {
        SETUP_UTILS.set(new SetupUtils());
        if( SETUP_UTILS.get() == null) throw new RuntimeException("This is null");
        SETUP_UTILS.get().startAllServices();
    }

    @AfterAll
    public static void tearDown() throws Exception {
        SETUP_UTILS.get().stopAllServices();
    }

    @Test
    public void basicWriterTest() throws IOException {
        String methodName = (new Object() {}).getClass().getEnclosingMethod().getName();
        log.info("Test case: {}", methodName);

        Path dir = Files.createTempDirectory("eo-ingestion-");

        FileSampleGenerator.FileGenerator.generate(dir, 10, 1000);

        PravegaSynchronizedWriter writer = new PravegaSynchronizedWriter(dir, SETUP_UTILS.get().getControllerUri(), methodName);
        writer.init();
        writer.run();

        Assertions.assertTrue(readEvents(writer.getController(),
                                        Stream.of(writer.getScope(), writer.getDatastream()),
                                        10000));
    }

    @Test
    public void failureDuringTxnWriterTest() throws IOException {
        String methodName = (new Object() {}).getClass().getEnclosingMethod().getName();
        log.info("Test case: {}", methodName);

        Path dir = Files.createTempDirectory("eo-ingestion-");

        FileSampleGenerator.FileGenerator.generate(dir, 50, 1000);

        PravegaSynchronizedWriter writerPreFailure = new PravegaSynchronizedWriter(dir, SETUP_UTILS.get().getControllerUri(), methodName);
        writerPreFailure.init();
        writerPreFailure.induceFailure(PravegaSynchronizedWriter.FType.DuringTxn);
        writerPreFailure.run();

        PravegaSynchronizedWriter writerPostFailure = new PravegaSynchronizedWriter(dir, SETUP_UTILS.get().getControllerUri(), methodName);
        writerPostFailure.init();
        writerPostFailure.run();

        Assertions.assertTrue(readEvents(writerPostFailure.getController(),
                                        Stream.of(writerPostFailure.getScope(), writerPostFailure.getDatastream()),
                                        50000));
    }

    @Test
    public void failureAfterCommitWriterTest() throws IOException {
        String methodName = (new Object() {}).getClass().getEnclosingMethod().getName();
        log.info("Test case: {}", methodName);

        Path dir = Files.createTempDirectory("eo-ingestion-");

        FileSampleGenerator.FileGenerator.generate(dir, 10, 1000);

        PravegaSynchronizedWriter writerPreFailure = new PravegaSynchronizedWriter(dir, SETUP_UTILS.get().getControllerUri(), methodName);
        writerPreFailure.init();
        writerPreFailure.induceFailure(PravegaSynchronizedWriter.FType.AfterCommit);
        writerPreFailure.run();

        PravegaSynchronizedWriter writerPostFailure = new PravegaSynchronizedWriter(dir, SETUP_UTILS.get().getControllerUri(), methodName);
        writerPostFailure.init();
        writerPostFailure.run();

        Assertions.assertTrue(readEvents(writerPostFailure.getController(),
                Stream.of(writerPostFailure.getScope(), writerPostFailure.getDatastream()),
                10000));
    }



    private boolean readEvents(URI controller, Stream stream, int total) {
        // Start reader group
        ClientConfig clientConfig = ClientConfig.builder()
                .controllerURI(controller)
                .build();

        ReaderGroupManager.withScope(stream.getScope(), controller)
                .createReaderGroup("group-" + stream.getStreamName(), ReaderGroupConfig.builder().stream(stream).build());

        int counter = 0;
        try (EventStreamClientFactory clientFactory = EventStreamClientFactory
                .withScope(stream.getScope(), clientConfig);
             EventStreamReader<Sample> reader = clientFactory.createReader("reader",
                     "group-" + stream.getStreamName(),
                     new AvroSampleSerializer(),
                     ReaderConfig.builder().build());) {

            EventRead<Sample> event;
            do {
                event = reader.readNextEvent(30000);

                if ((event.getEvent() != null) && (!event.isCheckpoint())) {
                    counter++;
                }
                log.info("Event read");
            } while ((event.getEvent() != null) || (event.isCheckpoint()));
        }

        log.info("Read total of {} events", counter);
        return (counter == total);
    }
}
