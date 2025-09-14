package org.seleznyov.iyu.kfin.ledger.application.service.waldisruptor.updated;

import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.util.HashMap;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Object pool for reducing GC pressure in high-frequency operations
 */
@Component
@Slf4j
public class ParameterMapPool {

    private final Queue<Map<String, Object>> pool = new ConcurrentLinkedQueue<>();
    private final AtomicLong created = new AtomicLong(0);
    private final AtomicLong acquired = new AtomicLong(0);
    private final AtomicLong released = new AtomicLong(0);

    private static final int MAX_POOL_SIZE = 1000;
    private static final int INITIAL_CAPACITY = 9;

    public Map<String, Object> acquire() {
        Map<String, Object> map = pool.poll();
        if (map == null) {
            map = new HashMap<>(INITIAL_CAPACITY);
            created.incrementAndGet();
        } else {
            map.clear(); // Ensure clean state
        }
        acquired.incrementAndGet();
        return map;
    }

    public void release(Map<String, Object> map) {
        if (map != null && pool.size() < MAX_POOL_SIZE) {
            map.clear();
            pool.offer(map);
            released.incrementAndGet();
        }
    }

    // Monitoring
    public long getCreated() {
        return created.get();
    }

    public long getAcquired() {
        return acquired.get();
    }

    public long getReleased() {
        return released.get();
    }

    public int getPoolSize() {
        return pool.size();
    }

    public double getHitRate() {
        long acq = acquired.get();
        return acq > 0 ? (double) (acq - created.get()) / acq * 100.0 : 0.0;
    }
}
