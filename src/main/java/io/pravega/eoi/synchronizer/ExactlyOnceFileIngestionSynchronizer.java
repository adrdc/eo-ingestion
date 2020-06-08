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

package io.pravega.eoi.synchronizer;

import io.pravega.avro.Status;
import io.pravega.client.SynchronizerClientFactory;
import io.pravega.client.state.*;
import lombok.Data;
import lombok.RequiredArgsConstructor;
import lombok.ToString;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.UUID;


/**
 * This class defines an updatable status to persist and maintain with the Pravega
 * {@link StateSynchronizer}. The {@link io.pravega.eoi.Status} is an avro object and
 * contains a file id and a txn id. The goal is to persist this status as a file
 * writer makes progress with ingesting events from a sequence of files. The files
 * are tobe ingested in order. In the case the writer restarts, it is able to resume
 * from the most recent status.
 *
 * This class defines a few important classes:
 *      - A status update called #StatusUpdate, and defines how the
 *      - An initial state class called #StatusInit
 *
 * The state synchronizer enables coordination across processes, but in this example,
 * we are illustrating its implementation using a single writer, which corresponds to
 * a single shard being consumed by a single writer. The example can be expanded to
 * assume multiple shards and associated writers.
 *
 */
@RequiredArgsConstructor
public class ExactlyOnceFileIngestionSynchronizer {
    static Logger log = LoggerFactory.getLogger(ExactlyOnceFileIngestionSynchronizer.class);

    private String scopedName;
    private Revision revision;
    private int sequence;
    private UUID txnId;

    /**
     * A private data class defining the revisioned state object that this synchronizer uses.
     */
    @Data
    private static class UpdatableStatus implements Revisioned {
        private final String streamName;
        private final Revision revision;
        private final Status status;


        @Override
        public String getScopedStreamName() {
            return streamName;
        }

        @Override
        public Revision getRevision() {
            return revision;
        }
    }

    @ToString
    @RequiredArgsConstructor
    static class StatusUpdate implements Update<UpdatableStatus>, Serializable {
        private final Status status;

        ByteBuffer getStatusBytes() throws IOException {
            return status.toByteBuffer();
        }

        @Override
        public UpdatableStatus applyTo(UpdatableStatus oldState, Revision newRevision) {
            log.debug("Applying update {} to {} ", this, oldState);

            /*
             * The new revision is given by the state synchronizer, and it is expected
             * to uniquely identify a revision of the state object for this stream. Under
             * the hood, this is the offset of the segment holding the data.
             */
            return new UpdatableStatus(oldState.streamName, newRevision, status);
        }

    }

    /**
     * Defines the initial status to be a file id of -1 and an arbitrary UUID.
     */
    @ToString
    static class StatusInit implements InitialUpdate<UpdatableStatus>, Serializable {
        private final Status status = Status.newBuilder()
                                            .setFileId(-1)
                                            .setTxnId(UUID.randomUUID().toString())
                                            .build();

        ByteBuffer getStatusBytes() throws IOException {
            return status.toByteBuffer();
        }

        @Override
        public UpdatableStatus create(String streamName, Revision revision) {
            return new UpdatableStatus(streamName, revision, status);
        }
    }


    /**
     * Private attribute holding a reference to the underlying state synchronizer
     * that this class builds upon.
     */
    private final StateSynchronizer<UpdatableStatus> stateSynchronizer;

    /**
     * Private constructor used by the static method
     * {@link this#createNewSynchronizer(String, SynchronizerClientFactory)}
     *
     * @param streamName
     * @param synchronizer
     */
    private ExactlyOnceFileIngestionSynchronizer(String streamName,
                                                 StateSynchronizer<UpdatableStatus> synchronizer) {
        this.stateSynchronizer = synchronizer;
        synchronizer.initialize(new StatusInit());
    }

    /**
     * Updates the state of this synchronizer by fetching the latest updates if any.
     */
    public void update() {
        stateSynchronizer.fetchUpdates();
    }

    /**
     * Returns the current status object.
     *
     * @return
     */
    public Status getStatus() { return stateSynchronizer.getState().status; }


    /**
     * Updates the status using the current expected one to spot concurrent access.
     *
     * @param newStatus
     * @param currentStatus
     * @return
     */
    public boolean updateStatus(Status newStatus, Status currentStatus) {
        return stateSynchronizer.updateState((current, updates)-> {
            /*
             * Validates that the current status is the expected one, no
             * concurrent updates.
             */

            if (currentStatus.getFileId() == current.status.getFileId() &&
                    currentStatus.getTxnId().equals(current.status.getTxnId())) {
                /*
                 * updates contains the list of updates to be applied; in this case,
                 * there is only one.
                 */
                updates.add(new StatusUpdate(newStatus));

                return true;
            } else {

                return false;
            }
        });
    }

    /**
     * Creates an instance of this status synchronizer.
     *
     * @param streamName Name of the stream to hold the data of this synchronizer
     * @param factory A factory for clients of the status synchronizer
     * @return A new instance of #ExactlyOnceFileIngestionSynchronizer
     */

    public static ExactlyOnceFileIngestionSynchronizer createNewSynchronizer(String streamName, SynchronizerClientFactory factory) {
        return new ExactlyOnceFileIngestionSynchronizer(streamName,
                factory.createStateSynchronizer(streamName,
                        new StatusUpdateSerializer(),
                        new StatusInitSerializer(),
                        SynchronizerConfig.builder().build()));
    }

}
