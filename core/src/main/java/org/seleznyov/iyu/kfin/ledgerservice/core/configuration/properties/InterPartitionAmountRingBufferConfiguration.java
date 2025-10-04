package org.seleznyov.iyu.kfin.ledgerservice.core.configuration.properties;

public record InterPartitionAmountRingBufferConfiguration(
    long recordsCount,
    int writeAttempts,
    int writeSpinAttempts,
    int writeYieldAttempts
) {

}
