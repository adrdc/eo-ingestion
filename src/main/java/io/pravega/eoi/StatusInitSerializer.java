package io.pravega.eoi;

import io.pravega.client.stream.Serializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;


public class StatusInitSerializer implements Serializer<ExactlyOnceIngestionSynchronizer.StatusInit> {
    static final Logger log = LoggerFactory.getLogger(io.pravega.eoi.StatusUpdateSerializer.class);

    @Override
    public ByteBuffer serialize(ExactlyOnceIngestionSynchronizer.StatusInit statusUpdate) {
        return ByteBuffer.allocate(0);
    }

    @Override
    public ExactlyOnceIngestionSynchronizer.StatusInit deserialize(ByteBuffer byteBuffer) {
            return new ExactlyOnceIngestionSynchronizer.StatusInit();
    }
}

