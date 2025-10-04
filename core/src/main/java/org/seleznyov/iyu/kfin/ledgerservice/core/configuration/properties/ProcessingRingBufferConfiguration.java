package org.seleznyov.iyu.kfin.ledgerservice.core.configuration.properties;

public record ProcessingRingBufferConfiguration(
    long entriesCount,
    int writeAttempts,
    int writeSpinAttempts,
    int writeYieldAttempts
) {

}
