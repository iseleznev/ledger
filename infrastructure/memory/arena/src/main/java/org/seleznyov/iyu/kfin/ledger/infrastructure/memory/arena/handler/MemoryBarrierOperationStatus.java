package org.seleznyov.iyu.kfin.ledger.infrastructure.memory.arena.handler;

public enum MemoryBarrierOperationStatus {
    HANDLING(0),
    COMPLETED(1);

    private final int memoryOrdinal;

    MemoryBarrierOperationStatus(int memoryOrdinal) {
        this.memoryOrdinal = memoryOrdinal;
    }

    public int memoryOrdinal() {
        return memoryOrdinal;
    }

    public static MemoryBarrierOperationStatus fromMemoryOrdinal(int memoryOrdinal) {
        for (MemoryBarrierOperationStatus status : MemoryBarrierOperationStatus.values()) {
            if (status.memoryOrdinal == memoryOrdinal) {
                return status;
            }
        }

        throw new IllegalArgumentException("Unknown memoryOrdinal: " + memoryOrdinal);
    }
}
