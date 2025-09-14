package org.seleznyov.iyu.kfin.ledger.shared.utils;

import java.util.UUID;

public class Uuid7Utils {

    private Uuid7Utils() {

    }

    public static long uuidToLong(UUID uuid) {
        return uuid.getMostSignificantBits() ^ uuid.getLeastSignificantBits();
    }
}
