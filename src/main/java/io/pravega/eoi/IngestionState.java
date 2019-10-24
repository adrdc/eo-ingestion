package io.pravega.eoi;

import io.pravega.client.state.InitialUpdate;
import io.pravega.client.state.Revision;
import io.pravega.client.state.Revisioned;
import io.pravega.client.state.Update;

import java.util.UUID;


public class IngestionState implements Revisioned{
    private String scopedName;
    private Revision revision;
    private int sequence;
    private UUID txnId;


    IngestionState(String scopedName, Revision revision) {
        this.scopedName = scopedName;
        this.revision = revision;
    }

    @Override
    public String getScopedStreamName() {
        return scopedName;
    }

    @Override
    public Revision getRevision() {
        return revision;
    }

    public static class IngestionStateInit implements InitialUpdate<IngestionState> {


        @Override
        public IngestionState create(String s, Revision revision) {
            return null;
        }

        @Override
        public IngestionState applyTo(IngestionState oldState, Revision newRevision) {
            return null;
        }
    }

    static class IngestionStateUpdate implements Update<IngestionState> {

        @Override
        public IngestionState applyTo(IngestionState oldState, Revision newRevision) {


            return oldState;
        }
    }
}
