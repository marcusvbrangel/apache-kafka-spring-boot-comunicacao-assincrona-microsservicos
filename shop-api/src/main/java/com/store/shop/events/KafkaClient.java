package com.store.shop.events;

import com.store.shop.dto.ShopDTO;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.Message;
import org.springframework.messaging.support.MessageBuilder;
import org.springframework.stereotype.Service;

@Slf4j
@Service
public class KafkaClient {

    private final KafkaTemplate<String, ShopDTO> kafkaTemplate;

    private static final String SHOP_TOPIC_NAME = "SHOP_TOPIC";

    public KafkaClient(KafkaTemplate<String, ShopDTO> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    public void sendMessage(ShopDTO shopDTO) {

        try {

            Message<ShopDTO> message = MessageBuilder
                    .withPayload(shopDTO)
                    .setHeader(KafkaHeaders.KEY, shopDTO.getBuyerIdentifier())   // garantia que mensagens com a mesma key vao para a mesma particao...
                    .setHeader("source", "shop-api")   // identificacao da origem da mensagem...
                    .setHeader(KafkaHeaders.TOPIC, SHOP_TOPIC_NAME)   // rastreabilidade...
                    .setHeader(KafkaHeaders.TIMESTAMP, System.currentTimeMillis())   // rastreabilidade...
                    .build();

            // kafkaTemplate.send(SHOP_TOPIC_NAME, shopDTO);
            kafkaTemplate.send(message);

        } catch (Exception ex) {
            log.error("Erro ao enviar mensagem para o kafka: {}", ex.getMessage());
            throw new RuntimeException("Erro ao enviar mensagem para o kafka", ex);
        }

    }

}
