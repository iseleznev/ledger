package org.seleznyov.iyu.kfin.ledgerservice.core.configuration.properties;

public record ActorTransferRingBufferConfiguration(
    long transfersCount,
    int writeAttempts,
    int writeSpinAttempts,
    int writeYieldAttempts
) {

}
