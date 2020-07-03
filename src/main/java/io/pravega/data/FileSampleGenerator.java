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
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.commons.cli.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Random;


public class FileSampleGenerator {
    static final Logger log = LoggerFactory.getLogger(FileSampleGenerator.class);

    public static class FileGenerator {
        public static void generate(Path path, int numOfFiles, int numOfRecords) {
            RandomSampleGenerator generator = new RandomSampleGenerator();

            for (int i = 0; i < numOfFiles; i++) {
                DatumWriter<Sample> sampleDatumWriter = new SpecificDatumWriter<>(Sample.class);
                DataFileWriter<Sample> dataFileWriter = new DataFileWriter<>(sampleDatumWriter);
                StringBuilder sb = new StringBuilder(Integer.toString(i));
                sb.append(".avro");

                try{
                    long initialTime = 0;
                    File f = new File(path.toFile(), sb.toString());
                    dataFileWriter.create(Sample.getClassSchema(), f);

                    log.info("File {}", f.toString());
                    int j = 0;
                    while (j++ < numOfRecords) {
                        Sample next = generator.getNext((long) i);
                        dataFileWriter.append(next);
                        log.debug(next.toString());
                    }

                    dataFileWriter.close();
                } catch(IOException e) {
                    log.error("IO exception while writing or closing file: {}", sb.toString(), e);
                    break;
                }
            }
        }

    }

    static class RandomSampleGenerator {
        private Random r = new Random();
        private int i = 0;

        Sample getNext(long time) {
            ByteBuffer stamp = ByteBuffer.allocate(Long.BYTES).putLong(time);
            stamp.flip();
            Sample s = Sample.newBuilder()
                    .setId(Integer.toString(r.nextInt()))
                    .setCounter(i++)
                    .setTimestamp(stamp)
                    .build();

            return s;
        }
    }

    /**
     * Input is:
     * 1- Directory path
     * 2- Number of files
     * 3- Number of records per file
     *
     * @param args
     */
    public static void main(String[] args) {
        int numOfFiles = 10;
        int numOfRecords = 10000;
        String namePrefix = "source";

        Options options = new Options();

        options.addOption("p", true, "directory path");
        options.addOption("f", true, "number of files");
        options.addOption("r", true, "records per file");

        CommandLineParser parser = new DefaultParser();
        CommandLine cmd;
        try {
            cmd = parser.parse(options, args);
        } catch (ParseException e) {
            log.error("Error while parsing options", e);
            return;
        }


        Path dir;
        if (cmd.hasOption("p")) {
            dir = Paths.get(cmd.getOptionValue("p"));
            if (!dir.toFile().exists()) {
                try {
                    Files.createDirectory(dir);
                } catch (IOException e) {
                    log.error("Error when creating directory", e);
                    return;
                }
            }
        } else {
            try {
                dir = Files.createTempDirectory("eo-ingestion-");
            } catch (IOException e) {
                log.error("Error while creating temporary repository", e);
                return;
            }
        }

        if (cmd.hasOption("f")) {
            numOfFiles = Integer.parseInt(cmd.getOptionValue("f"));
        }

        if (cmd.hasOption("r")) {
            numOfRecords = Integer.parseInt(cmd.getOptionValue("r"));
        }

        FileGenerator.generate(dir, numOfFiles, numOfRecords);
    }
}