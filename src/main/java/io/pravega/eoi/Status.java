package io.pravega.eoi;

import lombok.Data;

import java.io.Serializable;
import java.util.UUID;

@Data
public class Status implements Serializable {
    private final int fileId;
    private final UUID txnId;
}
