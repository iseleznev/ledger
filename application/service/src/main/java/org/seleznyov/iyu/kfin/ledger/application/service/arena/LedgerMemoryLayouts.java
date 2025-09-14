package org.seleznyov.iyu.kfin.ledger.application.service.arena;

import java.lang.foreign.MemoryLayout;

import static java.lang.foreign.MemoryLayout.sequenceLayout;
import static java.lang.foreign.MemoryLayout.structLayout;
import static java.lang.foreign.ValueLayout.*;

// Memory layouts for Arena
public class LedgerMemoryLayouts {

    // Entry layout: id(16) + accountId(16) + transactionId(16) + type(1) + amount(8) +
    // createdAt(8) + operationDate(4) + idempotencyKey(16) + currencyCode(8) + ordinal(8) = 101 bytes
    public static final MemoryLayout ENTRY_LAYOUT = structLayout(
        sequenceLayout(16, JAVA_BYTE).withName("id"),
        sequenceLayout(16, JAVA_BYTE).withName("accountId"),
        sequenceLayout(16, JAVA_BYTE).withName("transactionId"),
        JAVA_BYTE.withName("entryType"),
        JAVA_LONG.withName("amount"),
        JAVA_LONG.withName("createdAt"),
        JAVA_INT.withName("operationDate"),
        sequenceLayout(16, JAVA_BYTE).withName("idempotencyKey"),
        sequenceLayout(8, JAVA_BYTE).withName("currencyCode"),
        JAVA_LONG.withName("entryOrdinal")
    );

    // Snapshot layout: id(16) + accountId(16) + operationDate(4) + balance(8) +
    // lastEntryId(16) + lastOrdinal(8) + operationsCount(4) + snapshotOrdinal(8) + createdAt(8) = 88 bytes
    public static final MemoryLayout SNAPSHOT_LAYOUT = structLayout(
        sequenceLayout(16, JAVA_BYTE).withName("id"),
        sequenceLayout(16, JAVA_BYTE).withName("accountId"),
        JAVA_INT.withName("operationDate"),
        JAVA_LONG.withName("balance"),
        sequenceLayout(16, JAVA_BYTE).withName("lastEntryRecordId"),
        JAVA_LONG.withName("lastEntryOrdinal"),
        JAVA_INT.withName("operationsCount"),
        JAVA_LONG.withName("snapshotOrdinal"),
        JAVA_LONG.withName("createdAt")
    );

    public static final long ENTRY_SIZE = ENTRY_LAYOUT.byteSize();
    public static final long SNAPSHOT_SIZE = SNAPSHOT_LAYOUT.byteSize();
}
