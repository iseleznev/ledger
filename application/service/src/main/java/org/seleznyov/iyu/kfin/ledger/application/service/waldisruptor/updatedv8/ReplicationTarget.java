package org.seleznyov.iyu.kfin.ledger.application.service.waldisruptor.updatedv8;

import lombok.AllArgsConstructor;
import lombok.Data;

/**
 * Конфигурация узла репликации
 */
@Data
@AllArgsConstructor
public class ReplicationTarget {

    private final String name;
    private final String endpoint;
    private final Type type;

    public enum Type {
        SECONDARY,
        BACKUP
    }
}
