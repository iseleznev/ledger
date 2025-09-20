package org.seleznyov.iyu.kfin.ledger.infrastructure.memory.arena.configuration;

public record LedgerConfiguration(
    LedgerHotAccountsConfiguration hotAccounts,
    LedgerWarmAccountsConfiguration warmAccounts,
    LedgerEntriesConfiguration entries
) {

}
