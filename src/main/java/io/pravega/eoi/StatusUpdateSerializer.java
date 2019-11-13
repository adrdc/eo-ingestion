package io.pravega.eoi;

import io.pravega.avro.Status;
import io.pravega.client.stream.Serializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;

public class StatusUpdateSerializer implements Serializer<ExactlyOnceIngestionSynchronizer.StatusUpdate>{
    static final Logger log = LoggerFactory.getLogger(StatusUpdateSerializer.class);

    @Override
    public ByteBuffer serialize(ExactlyOnceIngestionSynchronizer.StatusUpdate statusUpdate) {
        try {
            return statusUpdate.getStatusBytes();
        } catch (IOException e) {
            log.error("Error while serializing Avro object", e);
            throw new RuntimeException(e);
        }
    }

    @Override
    public ExactlyOnceIngestionSynchronizer.StatusUpdate deserialize(ByteBuffer byteBuffer) {
        try {
            return new ExactlyOnceIngestionSynchronizer.StatusUpdate(Status.fromByteBuffer(byteBuffer));
        } catch (IOException e) {
            log.error("Error while deserializing Avro object", e);
            throw new RuntimeException(e);
        }
    }
}
