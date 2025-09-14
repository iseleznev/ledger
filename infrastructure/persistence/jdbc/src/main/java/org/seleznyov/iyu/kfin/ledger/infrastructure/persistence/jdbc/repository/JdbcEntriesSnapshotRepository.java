package org.seleznyov.iyu.kfin.ledger.infrastructure.persistence.jdbc.repository;

import lombok.RequiredArgsConstructor;
import org.seleznyov.iyu.kfin.ledger.domain.model.snapshot.EntriesSnapshot;
import org.seleznyov.iyu.kfin.ledger.infrastructure.persistence.api.repository.EntriesSnapshotRepository;
import org.seleznyov.iyu.kfin.ledger.infrastructure.persistence.jdbc.client.EntriesSnapshotClient;
import org.seleznyov.iyu.kfin.ledger.infrastructure.persistence.jdbc.client.EntriesSnapshotInsertClient;
import org.springframework.stereotype.Repository;

import java.time.LocalDate;
import java.util.Optional;
import java.util.UUID;

@Repository
@RequiredArgsConstructor
public class JdbcEntriesSnapshotRepository implements EntriesSnapshotRepository {

    private final EntriesSnapshotClient snapshotClient;
    private final EntriesSnapshotInsertClient insertClient;


    @Override
    public void insert(EntriesSnapshot entriesSnapshot) {
        insertClient.insert(entriesSnapshot);
    }

    @Override
    public EntriesSnapshot lastSnapshot(UUID accountId, LocalDate operationDate) {
        return snapshotClient.snapshot(accountId, operationDate);
    }

    @Override
    public Optional<EntriesSnapshot> findLatestSnapshot(UUID accountId, LocalDate operationDate) {
        return snapshotClient.findLatestSnapshot(accountId, operationDate);
    }

    @Override
    public Optional<EntriesSnapshot> findExactSnapshot(UUID accountId, LocalDate operationDate) {
        return snapshotClient.findExactSnapshot(accountId, operationDate);
    }
}
