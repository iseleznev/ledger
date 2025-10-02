package org.seleznyov.iyu.kfin.ledger.infrastructure.memory.arena.configuration;

public record PartitionActorConfiguration(
    int transfersCountQuota,
    int commitsCountQuota,
    int partitionAmountsCountQuota
) {

}
