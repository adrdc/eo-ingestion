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
import io.pravega.client.ClientConfig;
import io.pravega.client.EventStreamClientFactory;
import io.pravega.client.admin.StreamManager;
import io.pravega.client.stream.EventStreamWriter;
import io.pravega.client.stream.EventWriterConfig;
import io.pravega.client.stream.ScalingPolicy;
import io.pravega.client.stream.StreamConfiguration;
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


/**
 * This writer iterates over a set of files and writes the events in the files
 * to a Pravega stream. In the case of a restart, it makes no attempt to determine
 * where it stopped, and processes all files again.
 */

public class PravegaWriter {
    static final Logger log = LoggerFactory.getLogger(PravegaWriter.class);
    private Path path;
    private final String scope;
    private final String stream;
    private final URI controller;

    public PravegaWriter(Path path, URI controller) {
        this.path = path;
        this.scope = "test-scope";
        this.stream = "test-stream";
        this.controller = controller;
    }

    public void run() {

        /*
         * Creates stream to append file events to.
         */
        StreamManager streamManager = StreamManager.create(controller);
        streamManager.createScope(scope);

        StreamConfiguration streamConfig = StreamConfiguration.builder()
                .scalingPolicy(ScalingPolicy.fixed(10))
                .build();
        streamManager.createStream(scope, stream, streamConfig);

        ClientConfig clientConfig = ClientConfig.builder()
                .controllerURI(controller)
                .build();

        /*
         * Creates an event writer and iterates over the existing files
         * append the file events to the Pravega stream.
         */
        try (EventStreamClientFactory clientFactory = EventStreamClientFactory.withScope(scope, clientConfig);
             EventStreamWriter<Sample> writer = clientFactory.
             createEventWriter(stream,
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

                        /*
                         * Writes each avro object in a file to the Pravega stream
                         */
                        try {
                            DatumReader<Sample> userDatumReader = new SpecificDatumReader<>(Sample.class);
                            DataFileReader<Sample> dataFileReader = new DataFileReader<>(f, userDatumReader);
                            dataFileReader.forEach( sample -> writer.writeEvent(sample.getId().toString(), sample));

                        } catch (IOException e) {
                            throw new RuntimeException(e);
                        }
                    }
            );
        }
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
