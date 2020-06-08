package io.pravega.eoi.synchronizer;

import io.pravega.client.stream.Serializer;
import io.pravega.eoi.synchronizer.ExactlyOnceStreamIngestionSynchronizer.StreamPositionInit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;

public class StreamPositionInitSerializer implements Serializer<StreamPositionInit> {
    static final Logger log = LoggerFactory.getLogger(StreamPositionInitSerializer.class);
    @Override
    public ByteBuffer serialize(StreamPositionInit streamPositionInit) {
        return ByteBuffer.allocate(0);
    }

    @Override
    public StreamPositionInit deserialize(ByteBuffer byteBuffer) {
        return new StreamPositionInit();
    }
}
