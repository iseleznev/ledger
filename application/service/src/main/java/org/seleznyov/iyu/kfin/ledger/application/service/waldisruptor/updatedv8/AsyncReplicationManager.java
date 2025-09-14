package org.seleznyov.iyu.kfin.ledger.application.service.waldisruptor.updatedv8;

import lombok.extern.slf4j.Slf4j;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Async Replication Manager - высокая доступность
 */
@Slf4j
public class AsyncReplicationManager implements AutoCloseable {

    private final ExecutorService replicationExecutor =
        Executors.newFixedThreadPool(4, Thread.ofVirtual().name("replication-").factory());

    // Конфигурация узлов репликации
    private final List<ReplicationTarget> secondaryNodes;
    private final List<ReplicationTarget> backupTargets;

    // Статистика репликации
    private final AtomicLong replicationAttempts = new AtomicLong(0);
    private final AtomicLong replicationFailures = new AtomicLong(0);

    public AsyncReplicationManager() {
        // Инициализация узлов репликации из конфигурации
        this.secondaryNodes = initializeSecondaryNodes();
        this.backupTargets = initializeBackupTargets();

        log.info("Initialized replication manager with {} secondary nodes and {} backup targets",
            secondaryNodes.size(), backupTargets.size());
    }

    private List<ReplicationTarget> initializeSecondaryNodes() {
        return List.of(
            new ReplicationTarget("secondary-1", "tcp://replica1:9092", ReplicationTarget.Type.SECONDARY),
            new ReplicationTarget("secondary-2", "tcp://replica2:9092", ReplicationTarget.Type.SECONDARY)
        );
    }

    private List<ReplicationTarget> initializeBackupTargets() {
        return List.of(
            new ReplicationTarget("backup-s3", "s3://ledger-backup/", ReplicationTarget.Type.BACKUP),
            new ReplicationTarget("backup-region2", "tcp://region2:9092", ReplicationTarget.Type.BACKUP)
        );
    }

    public CompletableFuture<Void> replicateAsync(long offset, byte[] data) {
        return CompletableFuture.runAsync(() -> {
            replicationAttempts.incrementAndGet();

            try {
                List<CompletableFuture<Void>> replicationFutures = new ArrayList<>();

                // Replicate to secondary nodes
                for (ReplicationTarget target : secondaryNodes) {
                    replicationFutures.add(
                        CompletableFuture.runAsync(() -> replicateToSecondary(target, offset, data),
                            replicationExecutor));
                }

                // Replicate to backup targets
                for (ReplicationTarget target : backupTargets) {
                    replicationFutures.add(
                        CompletableFuture.runAsync(() -> replicateToBackup(target, offset, data),
                            replicationExecutor));
                }

                // Wait for completion with timeout
                CompletableFuture<Void> allReplications = CompletableFuture.allOf(
                    replicationFutures.toArray(new CompletableFuture[0]));

                try {
                    allReplications.get(5, TimeUnit.SECONDS);
                } catch (TimeoutException e) {
                    log.warn("Replication timeout for offset {}, some replicas may be behind", offset);
                }

            } catch (Exception e) {
                replicationFailures.incrementAndGet();
                log.error("Replication failed for offset {}", offset, e);
            }
        }, replicationExecutor);
    }

    private void replicateToSecondary(ReplicationTarget target, long offset, byte[] data) {
        try {
            if (target.getEndpoint().startsWith("tcp://")) {
                replicateViaTcp(target, offset, data);
            } else {
                throw new IllegalArgumentException("Unsupported secondary endpoint: " + target.getEndpoint());
            }

            log.debug("Successfully replicated offset {} to secondary {}", offset, target.getName());

        } catch (Exception e) {
            log.error("Failed to replicate to secondary {}: {}", target.getName(), e.getMessage());
            throw new RuntimeException("Secondary replication failed", e);
        }
    }

    private void replicateToBackup(ReplicationTarget target, long offset, byte[] data) {
        try {
            if (target.getEndpoint().startsWith("s3://")) {
                replicateToS3(target, offset, data);
            } else if (target.getEndpoint().startsWith("tcp://")) {
                replicateViaTcp(target, offset, data);
            } else if (target.getEndpoint().startsWith("file://")) {
                replicateToFileSystem(target, offset, data);
            } else {
                throw new IllegalArgumentException("Unsupported backup endpoint: " + target.getEndpoint());
            }

            log.debug("Successfully replicated offset {} to backup {}", offset, target.getName());

        } catch (Exception e) {
            log.error("Failed to replicate to backup {}: {}", target.getName(), e.getMessage());
        }
    }

    private void replicateViaTcp(ReplicationTarget target, long offset, byte[] data) throws IOException {
        String[] hostPort = target.getEndpoint().replace("tcp://", "").split(":");
        String host = hostPort[0];
        int port = Integer.parseInt(hostPort[1]);

        try (Socket socket = new Socket(host, port);
             DataOutputStream out = new DataOutputStream(socket.getOutputStream())) {

            out.writeUTF("REPLICATE");
            out.writeLong(offset);
            out.writeInt(data.length);
            out.write(data);
            out.flush();

            try (DataInputStream in = new DataInputStream(socket.getInputStream())) {
                String response = in.readUTF();
                if (!"ACK".equals(response)) {
                    throw new IOException("Unexpected response from " + target.getName() + ": " + response);
                }
            }
        }
    }

    private void replicateToS3(ReplicationTarget target, long offset, byte[] data) {
        // S3 replication placeholder
        log.debug("S3 replication not implemented yet for target {}", target.getName());
    }

    private void replicateToFileSystem(ReplicationTarget target, long offset, byte[] data) throws IOException {
        String basePath = target.getEndpoint().replace("file://", "");
        Path backupDir = Paths.get(basePath);

        Files.createDirectories(backupDir);

        Path backupFile = backupDir.resolve("offset-" + offset + ".data");
        try (FileChannel channel = FileChannel.open(backupFile,
            StandardOpenOption.CREATE, StandardOpenOption.WRITE, StandardOpenOption.TRUNCATE_EXISTING)) {

            ByteBuffer buffer = ByteBuffer.allocate(8 + data.length);
            buffer.putLong(offset);
            buffer.put(data);
            buffer.flip();

            channel.write(buffer);
            channel.force(true);
        }
    }

    public ReplicationStats getStats() {
        return new ReplicationStats(
            replicationAttempts.get(),
            replicationFailures.get(),
            secondaryNodes.size(),
            backupTargets.size()
        );
    }

    @Override
    public void close() {
        replicationExecutor.close();
        log.info("Closed replication manager. Final stats: {} attempts, {} failures",
            replicationAttempts.get(), replicationFailures.get());
    }
}
