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

import io.pravega.avro.Status;
import io.pravega.client.ClientConfig;
import io.pravega.client.SynchronizerClientFactory;
import io.pravega.client.admin.StreamManager;
import io.pravega.client.stream.ScalingPolicy;
import io.pravega.client.stream.StreamConfiguration;
import io.pravega.eoi.synchronizer.ExactlyOnceFileIngestionSynchronizer;
import io.pravega.utils.SetupUtils;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;
import java.util.concurrent.atomic.AtomicReference;

public class ExactlyOnceFileIngestionSynchronizerTest {

    static final Logger log = LoggerFactory.getLogger(ExactlyOnceFileIngestionSynchronizerTest.class);

    protected static final AtomicReference<SetupUtils> SETUP_UTILS = new AtomicReference();

    @BeforeAll
    public static void setup() throws Exception {
        SETUP_UTILS.set(new SetupUtils());
        if( SETUP_UTILS.get() == null) throw new RuntimeException("This is null");
        SETUP_UTILS.get().startAllServices();
    }

    @AfterAll
    public static void tearDown() throws Exception {
        SETUP_UTILS.get().stopAllServices();
    }

    @Test
    public void eoiSynchronizerTest() {
        URI controller = SETUP_UTILS.get().getControllerUri();
        String scope = "test-scope";
        String syncstream = "test-stream";
        ClientConfig clientConfig = ClientConfig.builder()
                .controllerURI(controller)
                .build();

        try (StreamManager streamManager = StreamManager.create(controller)) {
            streamManager.createScope(scope);

            // Create sync stream
            StreamConfiguration syncStreamConfig = StreamConfiguration.builder()
                    .scalingPolicy(ScalingPolicy.fixed(1))
                    .build();
            streamManager.createStream(scope, syncstream, syncStreamConfig);

            // Create state synchronizer
            SynchronizerClientFactory syncFactory = SynchronizerClientFactory.withScope(scope, clientConfig);
            ExactlyOnceFileIngestionSynchronizer synchronizer1 = ExactlyOnceFileIngestionSynchronizer.createNewSynchronizer(syncstream, syncFactory);
            ExactlyOnceFileIngestionSynchronizer synchronizer2 = ExactlyOnceFileIngestionSynchronizer.createNewSynchronizer(syncstream, syncFactory);

            Status status = synchronizer1.getStatus();
            Status newStatus = Status.newBuilder()
                    .setFileId(0)
                    .setTxnId("0x10")
                    .build();
            Assertions.assertTrue(synchronizer1.updateStatus(newStatus, status));
            Assertions.assertFalse(synchronizer2.updateStatus(newStatus, status));
        }
    }
}
