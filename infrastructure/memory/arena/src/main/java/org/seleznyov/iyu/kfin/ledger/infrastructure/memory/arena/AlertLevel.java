package org.seleznyov.iyu.kfin.ledger.infrastructure.memory.arena;

public enum AlertLevel {
    LOW("🟢", 1),
    MEDIUM("🟡", 2),
    HIGH("🟠", 3),
    CRITICAL("🔴", 4);

    private final String emoji;
    private final int priority;

    AlertLevel(String emoji, int priority) {
        this.emoji = emoji;
        this.priority = priority;
    }

    public String getEmoji() {
        return emoji;
    }

    public int getPriority() {
        return priority;
    }

    public boolean isHigherThan(AlertLevel other) {
        return this.priority > other.priority;
    }
}