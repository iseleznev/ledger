package org.seleznyov.iyu.kfin.ledgerservice.core.configuration.properties;

public record WalRingBufferConfiguration(
    long entriesCount,
    int writeAttempts,
    int writeSpinAttempts,
    int writeYieldAttempts
) {

}
