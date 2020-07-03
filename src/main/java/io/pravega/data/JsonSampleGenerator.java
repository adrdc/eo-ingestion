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

import com.fasterxml.jackson.databind.ObjectMapper;
import io.pravega.avro.Sample;
import io.pravega.client.ClientConfig;
import io.pravega.client.EventStreamClientFactory;
import io.pravega.client.admin.StreamManager;
import io.pravega.client.stream.EventStreamWriter;
import io.pravega.client.stream.EventWriterConfig;
import io.pravega.client.stream.ScalingPolicy;
import io.pravega.client.stream.StreamConfiguration;
import io.pravega.json.JsonSensorSample;
import io.pravega.json.JsonSensorSampleSerializer;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;
import java.time.Duration;
import java.util.Random;

public class JsonSampleGenerator implements AutoCloseable {

    static final Logger log = LoggerFactory.getLogger(StreamSampleGenerator.class);

    private final URI controller;
    private final String scope = "scope";
    private final String stream;
    private final Duration interval;
    private final int events;
    private final ObjectMapper mapper;

    public JsonSampleGenerator(URI controller, String stream, Duration interval, int events) {
        this.controller = controller;
        this.stream = stream;
        this.interval = interval;
        this.events = events;
        this.mapper = new ObjectMapper();
    }

    public void run() {
        // Create stream
        try(StreamManager streamManager = StreamManager.create(controller)) {
            streamManager.createScope(scope);

            log.info("Created scope");

            // Create data stream
            StreamConfiguration dataStreamConfig = StreamConfiguration.builder()
                    .scalingPolicy(ScalingPolicy.fixed(10))
                    .build();
            streamManager.createStream(scope, stream, dataStreamConfig);
            log.info("Created data stream");
        }

        // Create writer
        try(
            EventStreamClientFactory factory = EventStreamClientFactory
                    .withScope(scope, ClientConfig.builder().controllerURI(controller).build());
            EventStreamWriter writer = factory.createEventWriter(stream,
                    new JsonSensorSampleSerializer(),
                    EventWriterConfig.builder().build())) {

            log.info("Created writer");

            // Loop
            int counter = 0;
            Random random = new Random();
            int id;
            JsonSensorSample s;
            do {
                id = random.nextInt(10);
                s = JsonSensorSample.builder()
                        .id(id)
                        .measurement(counter)
                        .timestamp(System.currentTimeMillis())
                        .build();

                writer.writeEvent(Integer.toString(id), s);

                if (interval.toMillis() > 0) {
                    try {
                        Thread.sleep(interval.toMillis());
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        throw new RuntimeException(e);
                    }
                }
            } while (counter++ < (events - 1));
        }
        log.info("Finished run");
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

        if (cmd.hasOption("o")) {
            streamName = cmd.getOptionValue("o");
        }

        if (cmd.hasOption("t")) {
            interval = Duration.ofMillis(Long.valueOf(cmd.getOptionValue("t")));
        }

        if (cmd.hasOption("n")) {
            events = Integer.valueOf(cmd.getOptionValue("n"));
        }

        if (cmd.hasOption("c")) {
            controller = URI.create(cmd.getOptionValue("c"));
            log.info("Controller URI: {}", controller);

            try( JsonSampleGenerator generator =
                         new JsonSampleGenerator(controller, streamName, interval, events)) {
                generator.run();
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        } else {
            log.error("No controller URL provided");
        }
    }
}
