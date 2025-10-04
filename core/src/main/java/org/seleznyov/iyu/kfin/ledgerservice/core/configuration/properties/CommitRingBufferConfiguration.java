package org.seleznyov.iyu.kfin.ledgerservice.core.configuration.properties;

public record CommitRingBufferConfiguration(
    long commitsCount,
    int writeAttempts,
    int writeSpinAttempts,
    int writeYieldAttempts
) {

}
