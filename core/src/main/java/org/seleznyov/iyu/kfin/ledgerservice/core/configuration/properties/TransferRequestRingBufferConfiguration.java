package org.seleznyov.iyu.kfin.ledgerservice.core.configuration.properties;

public record TransferRequestRingBufferConfiguration(
    long transfersCount,
    int writeAttempts,
    int writeSpinAttempts,
    int writeYieldAttempts
) {

}
