package org.seleznyov.iyu.kfin.ledgerservice.core.configuration.properties;

public record AccountPartitionConfiguration(
    int partitionChunkAccountsCount,
    int partitionAccountsCount,
    int maxPsl
) {

}
