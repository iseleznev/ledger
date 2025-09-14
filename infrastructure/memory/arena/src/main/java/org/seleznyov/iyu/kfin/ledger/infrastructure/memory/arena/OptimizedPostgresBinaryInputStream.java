package org.seleznyov.iyu.kfin.ledger.infrastructure.memory.arena;

import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.io.InputStream;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;

/**
 * Полностью реализованный оптимизированный InputStream
 */
@Slf4j
public class OptimizedPostgresBinaryInputStream extends InputStream {

    private final MemorySegment dataSegment;
    private final byte[] postgresHeader;
    private final long totalSize;

    private long segmentPosition = 0;
    private int headerPosition = 0;
    private boolean headerSent = false;
    private boolean dataSent = false;
    private boolean terminatorSent = false;

    public OptimizedPostgresBinaryInputStream(MemorySegment dataSegment) {
        this.dataSegment = dataSegment;
        this.totalSize = dataSegment.byteSize();
        this.postgresHeader = generatePostgresBinaryHeader();

        log.trace("Created OptimizedPostgresBinaryInputStream: data_size={} bytes, header_size={} bytes",
            totalSize, postgresHeader.length);
    }

    @Override
    public int read() throws IOException {
        // 1. Отправляем PostgreSQL header
        if (!headerSent) {
            if (headerPosition < postgresHeader.length) {
                return postgresHeader[headerPosition++] & 0xFF;
            } else {
                headerSent = true;
            }
        }

        // 2. Отправляем данные из MemorySegment
        if (!dataSent) {
            int byteValue = readFromMemorySegment();
            if (byteValue != -1) {
                return byteValue;
            } else {
                dataSent = true;
            }
        }

        // 3. Отправляем terminator
        if (!terminatorSent) {
            long terminatorOffset = segmentPosition - totalSize;
            if (terminatorOffset < POSTGRES_TERMINATOR.length) {
                segmentPosition++;
                if (terminatorOffset == POSTGRES_TERMINATOR.length - 1) {
                    terminatorSent = true;
                }
                return POSTGRES_TERMINATOR[(int) terminatorOffset] & 0xFF;
            }
        }

        return -1; // EOF
    }

    @Override
    public int read(byte[] buffer, int offset, int length) throws IOException {
        if (length == 0) return 0;

        int totalRead = 0;

        // 1. Читаем header если нужно
        if (!headerSent) {
            int headerRemaining = postgresHeader.length - headerPosition;
            if (headerRemaining > 0) {
                int headerToRead = Math.min(headerRemaining, length);
                System.arraycopy(postgresHeader, headerPosition, buffer, offset, headerToRead);
                headerPosition += headerToRead;
                totalRead += headerToRead;

                if (headerPosition >= postgresHeader.length) {
                    headerSent = true;
                }

                if (totalRead == length) return totalRead;
            }
        }

        // 2. Bulk читаем данные из MemorySegment
        if (!dataSent && totalRead < length) {
            int dataRead = readBulkFromMemorySegment(buffer, offset + totalRead, length - totalRead);
            if (dataRead > 0) {
                totalRead += dataRead;
            } else {
                dataSent = true;
            }
        }

        // 3. Читаем terminator если нужно
        if (!terminatorSent && totalRead < length) {
            int terminatorRead = readTerminator(buffer, offset + totalRead, length - totalRead);
            totalRead += terminatorRead;
        }

        return totalRead > 0 ? totalRead : -1;
    }

    /**
     * Генерация PostgreSQL binary header
     */
    private byte[] generatePostgresBinaryHeader() {
        try {
            java.io.ByteArrayOutputStream baos = new java.io.ByteArrayOutputStream();
            java.io.DataOutputStream dos = new java.io.DataOutputStream(baos);

            // PostgreSQL binary copy signature (11 bytes)
            dos.write(POSTGRES_SIGNATURE);

            // Flags field (4 bytes) - 0 означает no special flags
            dos.writeInt(0);

            // Header extension area length (4 bytes) - 0 означает no extensions
            dos.writeInt(0);

            dos.close();
            byte[] header = baos.toByteArray();

            log.trace("Generated PostgreSQL binary header: {} bytes", header.length);
            return header;

        } catch (IOException e) {
            throw new RuntimeException("Failed to generate PostgreSQL binary header", e);
        }
    }

    /**
     * Чтение одного байта из MemorySegment
     */
    private int readFromMemorySegment() throws IOException {
        if (segmentPosition >= totalSize) {
            return -1; // EOF для data section
        }

        try {
            byte value = dataSegment.get(ValueLayout.JAVA_BYTE, segmentPosition);
            segmentPosition++;
            return value & 0xFF;

        } catch (Exception e) {
            throw new IOException("Error reading from MemorySegment at position " + segmentPosition, e);
        }
    }

    /**
     * Bulk чтение из MemorySegment в buffer
     */
    private int readBulkFromMemorySegment(byte[] buffer, int offset, int length) throws IOException {
        long remainingInSegment = totalSize - segmentPosition;

        if (remainingInSegment <= 0) {
            return 0; // Больше нет данных в segment
        }

        int toRead = (int) Math.min(remainingInSegment, length);

        try {
            // Bulk copy из MemorySegment в target buffer
            MemorySegment sourceSlice = dataSegment.asSlice(segmentPosition, toRead);
            MemorySegment targetSlice = MemorySegment.ofArray(buffer).asSlice(offset, toRead);

            MemorySegment.copy(sourceSlice, 0, targetSlice, 0, toRead);

            segmentPosition += toRead;

            log.trace("Bulk read {} bytes from MemorySegment, position now: {}", toRead, segmentPosition);
            return toRead;

        } catch (Exception e) {
            throw new IOException("Error in bulk read from MemorySegment: position=" +
                segmentPosition + ", length=" + toRead, e);
        }
    }

    /**
     * Чтение terminator bytes
     */
    private int readTerminator(byte[] buffer, int offset, int length) {
        long terminatorStart = totalSize;
        long terminatorPosition = segmentPosition - terminatorStart;

        if (terminatorPosition >= POSTGRES_TERMINATOR.length) {
            terminatorSent = true;
            return 0;
        }

        int terminatorRemaining = POSTGRES_TERMINATOR.length - (int) terminatorPosition;
        int toRead = Math.min(terminatorRemaining, length);

        System.arraycopy(POSTGRES_TERMINATOR, (int) terminatorPosition, buffer, offset, toRead);
        segmentPosition += toRead;

        if (terminatorPosition + toRead >= POSTGRES_TERMINATOR.length) {
            terminatorSent = true;
        }

        return toRead;
    }

    @Override
    public long skip(long n) throws IOException {
        long skipped = 0;
        byte[] skipBuffer = new byte[Math.min((int) n, 8192)];

        while (skipped < n) {
            int toSkip = (int) Math.min(n - skipped, skipBuffer.length);
            int actuallyRead = read(skipBuffer, 0, toSkip);
            if (actuallyRead == -1) break;
            skipped += actuallyRead;
        }

        return skipped;
    }

    @Override
    public int available() throws IOException {
        if (!headerSent) {
            return postgresHeader.length - headerPosition;
        } else if (!dataSent) {
            return (int) Math.min(Integer.MAX_VALUE, totalSize - segmentPosition);
        } else if (!terminatorSent) {
            return POSTGRES_TERMINATOR.length;
        }
        return 0;
    }

    @Override
    public void close() throws IOException {
        // MemorySegment не требует явного закрытия
        log.trace("OptimizedPostgresBinaryInputStream closed");
    }
}
