package org.seleznyov.iyu.kfin.ledger.infrastructure.memory.arena;


import lombok.extern.slf4j.Slf4j;

@Slf4j
public class NotificationChannels {
//    private static final Logger log = LoggerFactory.getLogger(NotificationChannels.class);

    @Value("${alerting.slack.webhook-url:}")
    private String slackWebhookUrl;

    @Value("${alerting.email.enabled:false}")
    private boolean emailEnabled;

    @Value("${alerting.sms.enabled:false}")
    private boolean smsEnabled;

    private final RestTemplate restTemplate;
    private final JavaMailSender mailSender;

    public NotificationChannels(
        RestTemplate restTemplate,
        @Autowired(required = false) JavaMailSender mailSender
    ) {
        this.restTemplate = restTemplate;
        this.mailSender = mailSender;
    }

    public void sendToSlack(AlertPayload payload) {
        if (slackWebhookUrl.isEmpty()) {
            log.debug("Slack webhook not configured");
            return;
        }

        try {
            SlackMessage slack = SlackMessage.builder()
                .text(formatSlackMessage(payload))
                .username("Ledger-Service-Bot")
                .iconEmoji(":warning:")
                .build();

            restTemplate.postForObject(slackWebhookUrl, slack, String.class);
            log.debug("Sent alert to Slack");

        } catch (Exception e) {
            log.error("Failed to send Slack notification", e);
        }
    }

    public void sendToEmail(AlertPayload payload) {
        if (!emailEnabled || mailSender == null) {
            log.debug("Email alerting not configured");
            return;
        }

        try {
            SimpleMailMessage message = new SimpleMailMessage();
            message.setTo("dev-team@yourcompany.com");
            message.setSubject(String.format("[%s] %s Alert - Ledger Service",
                payload.getLevel().name(), payload.getLevel().getEmoji()));
            message.setText(formatEmailMessage(payload));

            mailSender.send(message);
            log.debug("Sent alert via email");

        } catch (Exception e) {
            log.error("Failed to send email notification", e);
        }
    }

    public void sendToSms(AlertPayload payload) {
        if (!smsEnabled) {
            log.debug("SMS alerting not configured");
            return;
        }

        // Здесь интеграция с SMS провайдером (Twilio, AWS SNS, etc.)
        log.info("SMS alert would be sent: {}", payload.getMessage());
    }

    public void sendToTelegram(AlertPayload payload) {
        // Интеграция с Telegram Bot API
        log.info("Telegram alert would be sent: {}", payload.getMessage());
    }

    private String formatSlackMessage(AlertPayload payload) {
        StringBuilder sb = new StringBuilder();
        sb.append(payload.getLevel().getEmoji()).append(" *")
            .append(payload.getLevel().name()).append(" Alert*\n");
        sb.append("*Message:* ").append(payload.getMessage()).append("\n");
        sb.append("*Environment:* ").append(payload.getEnvironment()).append("\n");
        sb.append("*Time:* ").append(payload.getTimestamp()).append("\n");

        if (!payload.getDetails().isEmpty()) {
            sb.append("*Details:*\n");
            payload.getDetails().forEach((key, value) ->
                sb.append("• ").append(key).append(": ").append(value).append("\n"));
        }

        return sb.toString();
    }

    private String formatEmailMessage(AlertPayload payload) {
        return String.format("""
                Alert Level: %s
                Message: %s
                Environment: %s
                Timestamp: %s
                
                Details:
                %s
                
                --
                Ledger Service Alert System
                """,
            payload.getLevel(),
            payload.getMessage(),
            payload.getEnvironment(),
            payload.getTimestamp(),
            payload.getDetails().entrySet().stream()
                .map(entry -> entry.getKey() + ": " + entry.getValue())
                .collect(Collectors.joining("\n"))
        );
    }
}