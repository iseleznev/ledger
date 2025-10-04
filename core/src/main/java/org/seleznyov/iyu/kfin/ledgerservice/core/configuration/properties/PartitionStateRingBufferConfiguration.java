package org.seleznyov.iyu.kfin.ledgerservice.core.configuration.properties;

public record PartitionStateRingBufferConfiguration(
    long entriesCount,
    int writeAttempts,
    int writeSpinAttempts,
    int writeYieldAttempts
) {

}
