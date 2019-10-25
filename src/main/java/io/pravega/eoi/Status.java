package io.pravega.eoi;

import lombok.Data;

import java.util.UUID;

@Data
public class Status {
    private final int fileId;
    private final UUID txnId;
}
