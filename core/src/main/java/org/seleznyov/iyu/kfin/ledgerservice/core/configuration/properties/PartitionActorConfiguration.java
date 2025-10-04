package org.seleznyov.iyu.kfin.ledgerservice.core.configuration.properties;

public record PartitionActorConfiguration(
    int transfersCountQuota,
    int commitsCountQuota,
    int partitionAmountsCountQuota
) {

}
