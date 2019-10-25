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
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Random;


public class FileSampleGenerator {
    static final Logger log = LoggerFactory.getLogger(FileSampleGenerator.class);


    static class FileGenerator {
        private Path path;
        private int numOfFiles;
        private int numOfRecords;

        FileGenerator(Path path, int numOfFiles, int numOfRecords) {
            this.path = path;
            this.numOfFiles = numOfFiles;
            this.numOfRecords = numOfRecords;
        }

        void generate() {
            RandomSampleGenerator generator = new RandomSampleGenerator();

            for (int i = 0; i < numOfFiles; i++) {
                DatumWriter<Sample> sampleDatumWriter = new SpecificDatumWriter<>(Sample.class);
                DataFileWriter<Sample> dataFileWriter = new DataFileWriter<>(sampleDatumWriter);
                StringBuilder sb = new StringBuilder(Integer.toString(i));
                sb.append(".avro");

                try{
                    File f = new File(path.toFile(), sb.toString());
                    dataFileWriter.create(Sample.getClassSchema(), f);

                    log.info("File {}", f.toString());
                    int j = 0;
                    while (j++ < numOfRecords) {
                        dataFileWriter.append(generator.getNext());
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

        Sample getNext() {
            Sample s = Sample.newBuilder()
                    .setId(Integer.toString(r.nextInt()))
                    .setCounter(i++)
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
            numOfRecords = Integer.parseInt("r");
        }

        FileGenerator fileGenerator = new FileGenerator(dir, numOfFiles, numOfRecords);
        fileGenerator.generate();

    }
}