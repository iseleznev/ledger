package org.seleznyov.iyu.kfin.ledgerservice.core.configuration.properties;

public record PartitionAmountRingBufferConfiguration(
    long recordsCount,
    int writeAttempts,
    int writeSpinAttempts,
    int writeYieldAttempts
) {

}
