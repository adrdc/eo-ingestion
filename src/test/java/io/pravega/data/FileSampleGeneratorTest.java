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
import io.pravega.data.FileSampleGenerator.FileGenerator;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.io.DatumReader;
import org.apache.avro.specific.SpecificDatumReader;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;

public class FileSampleGeneratorTest {
    static final Logger log = LoggerFactory.getLogger(FileSampleGeneratorTest.class);
    Path path;
    int numOfFiles;
    int numOfRecords;

    @BeforeEach
    public void setUp() throws IOException {
        this.path = Files.createTempDirectory("eo-ingestion-");
        this.numOfFiles = 10;
        this.numOfRecords = 1000;
    }

    @AfterEach
    public void tearDown() {
        Arrays.stream(path.toFile().listFiles()).forEach(f -> f.delete());
    }

    @Test
    public void testFileSampleGenerator() {
        // Order and read files
        FileGenerator.generate(this.path,
                                this.numOfFiles,
                                this.numOfRecords);
        log.info("Path {}", path.toString());
        Assertions.assertEquals(path.toFile().listFiles().length, this.numOfFiles);
        Arrays.stream(path.toFile().listFiles()).forEach(
                f -> {
                    try {
                        DatumReader<Sample> userDatumReader = new SpecificDatumReader<>(Sample.class);
                        DataFileReader<Sample> dataFileReader = new DataFileReader<>(f, userDatumReader);
                        Sample sample = null;
                        int counter = 0;
                        while (dataFileReader.hasNext()) {
                            sample = dataFileReader.next(sample);
                            counter++;
                        }

                        Assertions.assertEquals(counter, numOfRecords);
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                }
        );
    }
}
