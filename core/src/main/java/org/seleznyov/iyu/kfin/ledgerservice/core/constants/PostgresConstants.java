package org.seleznyov.iyu.kfin.ledgerservice.core.constants;

import java.lang.foreign.MemorySegment;

public interface PostgresConstants {
    int POSTGRES_SIGNATURE_OFFSET = 0;             // 11 bytes "PGCOPY\n\377\r\n\0"
    int POSTGRES_FLAGS_OFFSET = 11;                // 4 bytes
    int POSTGRES_EXTENSION_OFFSET = 15;            // 4 bytes
    int POSTGRES_HEADER_SIZE = 19;

    byte[] POSTGRES_SIGNATURE = "PGCOPY\n\377\r\n\0".getBytes();

    MemorySegment POSTGRES_SIGNATURE_MEMORY_SEGMENT = MemorySegment.ofArray(POSTGRES_SIGNATURE);
}
