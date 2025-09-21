package org.seleznyov.iyu.kfin.ledger.infrastructure.memory.arena;

import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;

import java.util.concurrent.Executor;

//@Configuration
//@EnableAsync
//@EnableScheduling
public class AlertingConfig {

//    @Bean
    public RestTemplate restTemplate() {
        return new RestTemplate();
    }

//    @Bean("alertExecutor")
    public Executor alertExecutor() {
        ThreadPoolTaskExecutor executor = new ThreadPoolTaskExecutor();
        executor.setCorePoolSize(2);
        executor.setMaxPoolSize(5);
        executor.setQueueCapacity(100);
        executor.setThreadNamePrefix("alert-");
        executor.initialize();
        return executor;
    }
}