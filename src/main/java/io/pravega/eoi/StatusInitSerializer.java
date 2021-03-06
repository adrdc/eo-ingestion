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

