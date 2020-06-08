package io.pravega.eoi.synchronizer;

import io.pravega.avro.StreamPosition;
import io.pravega.client.stream.Serializer;
import io.pravega.eoi.synchronizer.ExactlyOnceStreamIngestionSynchronizer.StreamPositionUpdate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;

public class StreamPositionUpdateSerializer implements Serializer<StreamPositionUpdate> {
    static final Logger log = LoggerFactory.getLogger(StreamPositionUpdateSerializer.class);

    @Override
    public ByteBuffer serialize(StreamPositionUpdate streamPositionUpdate) {
        try {
            return streamPositionUpdate.getStreamPositionBytes();
        } catch (IOException e) {
            log.error("Error while serializing Avro object", e);
            throw new RuntimeException(e);
        }
    }

    @Override
    public StreamPositionUpdate deserialize(ByteBuffer byteBuffer) {
        try {
            return new StreamPositionUpdate(StreamPosition.fromByteBuffer(byteBuffer));
        } catch (IOException e) {
            log.error("Error while deserializing Avro object", e);
            throw new RuntimeException(e);
        }
    }
}
