package com.store.shop.events;

import com.store.shop.dto.ShopDTO;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.annotation.RetryableTopic;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.retry.annotation.Backoff;
import org.springframework.stereotype.Service;

@Slf4j
@Service
public class ReceiveKafkaMessage {

    private final KafkaTemplate<String, ShopDTO> kafkaTemplate;

    private static final String SHOP_TOPIC = "SHOP_TOPIC";
    private static final String SHOP_TOPIC_RETRY = "SHOP_TOPIC_RETRY";

    public ReceiveKafkaMessage(KafkaTemplate<String, ShopDTO> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    @KafkaListener(topics = SHOP_TOPIC, groupId = "group-retry")
    public void listenShopTopic(ShopDTO shopDTO,
                                Acknowledgment acknowledgment,
                                @Header(name = KafkaHeaders.RECEIVED_KEY, required = false) String key,
                                @Header(name = KafkaHeaders.RECEIVED_TOPIC, required = false) String topic,
                                @Header(name = KafkaHeaders.RECEIVED_PARTITION, required = false) Integer partitionId) throws Exception {

        log.info("Shop Retry :: Tentativa 1/1 - Compra recebida no tópico: {} - ID: {} - key: {} - partition: {}",
                topic, shopDTO.getIdentifier(), key, partitionId);

        if (shopDTO.getItems() == null || shopDTO.getItems().isEmpty()) {
            log.error("Shop Retry :: Tentativa 1/1 - ERRO: Compra sem itens - ID: {}", shopDTO.getIdentifier());
            // Envia para tópico de retry
            kafkaTemplate.send(SHOP_TOPIC_RETRY, shopDTO);
            acknowledgment.acknowledge();
            return;
        }

        log.info("Shop Retry :: Tentativa 1/1 - Compra processada com sucesso - ID: {}", shopDTO.getIdentifier());
        acknowledgment.acknowledge();
    }

    @RetryableTopic(
        attempts = "10",
        backoff = @Backoff(delay = 2000)
    )
    @KafkaListener(topics = SHOP_TOPIC_RETRY, groupId = "group-retry-dlq")
    public void listenShopTopicRetry(ShopDTO shopDTO,
                                     Acknowledgment acknowledgment,
                                     @Header(name = KafkaHeaders.RECEIVED_KEY, required = false) String key,
                                     @Header(name = KafkaHeaders.RECEIVED_PARTITION, required = false) Integer partitionId,
                                     @Header(name = "kafka_receivedTopic", required = false) String receivedTopic,
                                     @Header(name = "kafka_receivedTimestamp", required = false) Long timestamp) throws Exception {

        String topic = receivedTopic != null ? receivedTopic : "SHOP_TOPIC_RETRY";

        log.info("Shop Retry :: Reprocessamento - Compra recebida no tópico de retry - ID: {} - key: {} - partition: {} - topic: {}",
                shopDTO.getIdentifier(), key, partitionId, topic);

        if (shopDTO.getItems() == null || shopDTO.getItems().isEmpty()) {
            log.error("Shop Retry :: Reprocessamento - ERRO: Compra sem itens - ID: {}", shopDTO.getIdentifier());
            throw new Exception("Itens da compra não podem estar vazios");
        }

        log.info("Shop Retry :: Reprocessamento - Compra processada com sucesso - ID: {}", shopDTO.getIdentifier());
        acknowledgment.acknowledge();
    }

}
