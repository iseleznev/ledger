package org.seleznyov.iyu.kfin.ledger.infrastructure.memory.arena;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import org.springframework.stereotype.Component;

@Component
public class StripeMetrics {
    private final MeterRegistry meterRegistry;

    // Счетчики
    private final Counter[] threadCrashes;
    private final Counter[] queueOverflows;
    private final Counter[] callerRunsExecutions;
    private final Counter[] callerRunsFailures;
    private final Counter[] rejectedTasks;

    // Таймеры
    private final Timer[] operationTimers;

    public StripeMetrics(MeterRegistry meterRegistry, int workerCount) {
        this.meterRegistry = meterRegistry;

        // Инициализация метрик для каждого воркера
        this.threadCrashes = new Counter[workerCount];
        this.queueOverflows = new Counter[workerCount];
        this.callerRunsExecutions = new Counter[workerCount];
        this.callerRunsFailures = new Counter[workerCount];
        this.rejectedTasks = new Counter[workerCount];
        this.operationTimers = new Timer[workerCount];

        for (int i = 0; i < workerCount; i++) {
            String workerTag = String.valueOf(i);

            threadCrashes[i] = Counter.builder("stripe.thread.crashes")
                .tag("workerId", workerTag)
                .description("Number of thread crashes in stripe worker")
                .register(meterRegistry);

            queueOverflows[i] = Counter.builder("stripe.queue.overflows")
                .tag("workerId", workerTag)
                .description("Number of queue overflows in stripe worker")
                .register(meterRegistry);

            callerRunsExecutions[i] = Counter.builder("stripe.caller_runs.executions")
                .tag("workerId", workerTag)
                .description("Number of tasks executed in caller thread")
                .register(meterRegistry);

            callerRunsFailures[i] = Counter.builder("stripe.caller_runs.failures")
                .tag("workerId", workerTag)
                .description("Number of failed caller runs executions")
                .register(meterRegistry);

            rejectedTasks[i] = Counter.builder("stripe.tasks.rejected")
                .tag("workerId", workerTag)
                .description("Number of completely rejected tasks")
                .register(meterRegistry);

            operationTimers[i] = Timer.builder("stripe.operation.duration")
                .tag("workerId", workerTag)
                .description("Duration of stripe operations")
                .register(meterRegistry);
        }
    }

    // Методы для инкремента счетчиков
    public void incrementThreadCrashes(int workerIndex) {
        threadCrashes[workerIndex].increment();
    }

    public void incrementQueueOverflow(int workerIndex) {
        queueOverflows[workerIndex].increment();
    }

    public void incrementCallerRunsExecutions(int workerIndex) {
        callerRunsExecutions[workerIndex].increment();
    }

    public void incrementCallerRunsFailures(int workerIndex) {
        callerRunsFailures[workerIndex].increment();
    }

    public void incrementRejectedTasks(int workerIndex) {
        rejectedTasks[workerIndex].increment();
    }

    public Timer.Sample startTimer(int workerIndex) {
        return Timer.start(meterRegistry);
    }

    public void recordOperation(int workerIndex, Timer.Sample sample) {
        sample.stop(operationTimers[workerIndex]);
    }

    // Gauge'ы для live данных будут добавлены в мониторе
}