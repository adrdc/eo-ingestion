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

package io.pravega.json;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.pravega.client.stream.Serializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;

public class JsonSensorSampleSerializer implements Serializer<JsonSensorSample> {

    static final Logger log = LoggerFactory.getLogger(JsonSensorSampleSerializer.class);

    private final ObjectMapper objectMapper = new ObjectMapper();

    @Override
    public ByteBuffer serialize(JsonSensorSample sample) {
        try {
            log.info("{}", objectMapper.writeValueAsString(sample));
            return ByteBuffer.wrap(objectMapper.writeValueAsBytes(sample));
        } catch (JsonProcessingException e) {
            log.error("Error processing sample: %s", sample, e);
            throw new RuntimeException(e);
        }
    }

    @Override
    public JsonSensorSample deserialize(ByteBuffer byteBuffer) {
        try {
            return objectMapper.readValue(byteBuffer.array(), JsonSensorSample.class);
        } catch (IOException e) {
            log.error("Error processing sample", e);
            throw new RuntimeException(e);
        }
    }
}
