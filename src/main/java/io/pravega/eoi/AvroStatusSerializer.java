package io.pravega.eoi;

import io.pravega.avro.Status;
import io.pravega.client.stream.Serializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;

public class AvroStatusSerializer implements Serializer<Status> {
    static final Logger log = LoggerFactory.getLogger(AvroStatusSerializer.class);

    @Override
    public ByteBuffer serialize(Status status) {
        try {
            return status.toByteBuffer();
        } catch (IOException e) {
            log.error("Error while serializing Avro object", e);
            throw new RuntimeException(e);
        }
    }

    @Override
    public Status deserialize(ByteBuffer byteBuffer) {
        try {
            return Status.fromByteBuffer(byteBuffer);
        } catch (IOException e) {
            log.error("Error while deserializing Avro object", e);
            throw new RuntimeException(e);
        }
    }
}
