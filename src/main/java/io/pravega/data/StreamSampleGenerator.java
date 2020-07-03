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
package io.pravega.data;

import io.pravega.avro.Sample;
import io.pravega.client.ClientConfig;
import io.pravega.client.EventStreamClientFactory;
import io.pravega.client.admin.StreamManager;
import io.pravega.client.stream.EventStreamWriter;
import io.pravega.client.stream.EventWriterConfig;
import io.pravega.client.stream.ScalingPolicy;
import io.pravega.client.stream.StreamConfiguration;
import io.pravega.eoi.AvroSampleSerializer;
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
import java.util.Random;

public class StreamSampleGenerator implements AutoCloseable {
    static final Logger log = LoggerFactory.getLogger(StreamSampleGenerator.class);

    private final URI controller;
    private final String scope = "scope";
    private final String stream;
    private final Duration interval;
    private final int events;

    public StreamSampleGenerator(URI controller, String stream, Duration interval, int events) {
        this.controller = controller;
        this.stream = stream;
        this.interval = interval;
        this.events = events;
    }

    public void run() {
        // Create stream
        StreamManager streamManager = StreamManager.create(controller);
        streamManager.createScope(scope);

        // Create data stream
        StreamConfiguration dataStreamConfig = StreamConfiguration.builder()
                    .scalingPolicy(ScalingPolicy.fixed(10))
                    .build();
        streamManager.createStream(scope, stream, dataStreamConfig);

        // Create writer
        EventStreamClientFactory factory = EventStreamClientFactory
                .withScope(scope, ClientConfig.builder().controllerURI(controller).build());
        EventStreamWriter writer = factory.createEventWriter(stream,
                new AvroSampleSerializer(),
                EventWriterConfig.builder().build());

        // Loop
        int counter = 0;
        Random random = new Random();
        int id;
        Sample s;
        do {
            id = random.nextInt();
            s = Sample.newBuilder()
                    .setId(Integer.toString(id))
                    .setCounter(counter)
                    .setTimestamp(ByteBuffer.allocate(Long.BYTES).putLong(System.currentTimeMillis()))
                    .build();

            writer.writeEvent(Integer.toString(id), s);
        } while (counter++ < events);
    }


    @Override
    public void close() throws Exception {

    }

    public static void main(String[] args) {
        URI controller;
        String streamName = "test";
        Duration interval = Duration.ofMillis(1000);
        int events = Integer.MAX_VALUE;

        Options options = new Options();

        options.addOption("c", true, "Controller URI");
        options.addOption("o", true, "Output stream");
        options.addOption("t", true, "Interval between events in milliseconds");
        options.addOption("n", true, "Number of events");


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

        if (cmd.hasOption("t")) {
            interval = Duration.ofMillis(Long.valueOf(cmd.getOptionValue("t")));
        }

        if (cmd.hasOption("n")) {
            events = Integer.getInteger(cmd.getOptionValue("n"));
        }

        if (cmd.hasOption("c")) {
            controller = URI.create(cmd.getOptionValue("c"));
            log.info("Controller URI: {}", controller);

            try( StreamSampleGenerator generator =
                         new StreamSampleGenerator(controller, streamName, interval, events)) {
               generator.run();
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        } else {
            log.error("No controller URL provided");
        }
    }
}
