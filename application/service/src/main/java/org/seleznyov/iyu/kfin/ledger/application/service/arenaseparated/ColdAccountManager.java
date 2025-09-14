package org.seleznyov.iyu.kfin.ledger.application.service.arenaseparated;

import java.util.concurrent.CompletableFuture;

// Cold Account Interface - Asynchronous for resource efficiency
public interface ColdAccountManager extends BaseAccountManager {

    /**
     * Add entry asynchronously - optimized for many low-RPS accounts
     *
     * @param entry Entry to add
     * @return CompletableFuture<Boolean> indicating success
     */
    CompletableFuture<Boolean> addEntry(EntryRecord entry);
}
