package org.seleznyov.iyu.kfin.ledger.domain.model.entryrecord;

public enum EntryType {
    DEBIT,
    CREDIT;

    public static EntryType ofShort(short ordinalValue) {
        if (ordinalValue < 0) {
            return DEBIT;
        } else {
            return CREDIT;
        }
    }

    public short shortFromEntryType() {
        if (DEBIT.equals(this)) {
            return -1;
        } else {
            return 1;
        }
    }
}
