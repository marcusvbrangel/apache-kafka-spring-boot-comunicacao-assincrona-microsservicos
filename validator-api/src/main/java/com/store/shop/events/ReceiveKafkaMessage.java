package com.store.shop.events;

import com.store.shop.dto.ShopDTO;
import com.store.shop.dto.ShopItemDTO;
import com.store.shop.model.Product;
import com.store.shop.repository.ProductRepository;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.Message;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.support.MessageBuilder;
import org.springframework.stereotype.Service;

@Slf4j
@Service
public class ReceiveKafkaMessage {

    private static final String SHOP_TOPIC_NAME = "SHOP_TOPIC";
    private static final String SHOP_TOPIC_EVENT_NAME = "SHOP_TOPIC_EVENT";
    private static final String SHOP_STATUS_ERROR = "ERROR";
    private static final String SHOP_STATUS_SUCCESS = "SUCCESS";

    private final ProductRepository productRepository;
    private final KafkaTemplate<String, ShopDTO> kafkaTemplate;

    public ReceiveKafkaMessage(ProductRepository productRepository, KafkaTemplate<String, ShopDTO> kafkaTemplate) {
        this.productRepository = productRepository;
        this.kafkaTemplate = kafkaTemplate;
    }

    @KafkaListener(topics = SHOP_TOPIC_NAME, groupId = "validation-validate-shop-group")
    public void listenShopTopic(ShopDTO shopDTO,
                                Acknowledgment acknowledgment,
                                @Header(name = KafkaHeaders.RECEIVED_KEY, required = false) String key,
                                @Header(name = KafkaHeaders.RECEIVED_TOPIC, required = false) String topic,
                                @Header(name = KafkaHeaders.RECEIVED_PARTITION, required = false) Integer partitionId,
                                @Header(name = KafkaHeaders.RECEIVED_TIMESTAMP, required = false) Long timeStamp
    ) {

        try {

            log.info("-----------------------------------------------");
            log.info("Compra recebida");
            log.info("key: {}", key);
            log.info("topic: {}", topic);
            log.info("partitionId: {}", partitionId);
            log.info("timeStamp: {}", timeStamp);
            log.info("-----------------------------------------------");

            boolean success = true;

            for (ShopItemDTO item : shopDTO.getItems()) {

                Product product = productRepository.findByIdentifier(item.getProductIdentifier());

                if (!isValidShop(item, product)) {
                    sendMessage(shopDTO, SHOP_STATUS_ERROR);
                    success = false;
                    break;
                }

            }

            if (success) {
                sendMessage(shopDTO, SHOP_STATUS_SUCCESS);
            }

            acknowledgment.acknowledge();

        } catch (Exception ex) {
            log.error("Erro no processamento da compra: {}", shopDTO.getIdentifier());
        }

    }

    private void sendMessage(ShopDTO shopDTO, String status) {

        try {

            if (SHOP_STATUS_SUCCESS.equals(status)) {
                log.info("Compra efetuada com sucesso: {}", shopDTO.getIdentifier());
                shopDTO.setStatus(SHOP_STATUS_SUCCESS);
            } else {
                log.info("Erro no processamento da compra: {}", shopDTO.getIdentifier());
                shopDTO.setStatus(SHOP_STATUS_ERROR);
            }

            Message<ShopDTO> message = MessageBuilder
                    .withPayload(shopDTO)
                    .setHeader("source", "validator-api")
                    .setHeader(KafkaHeaders.TOPIC, SHOP_TOPIC_EVENT_NAME)
                    .setHeader(KafkaHeaders.TIMESTAMP, System.currentTimeMillis())
                    .build();

            kafkaTemplate.send(message);


        } catch (Exception ex) {
            log.error("Erro ao enviar mensagem para o kafka: {}", ex.getMessage());
            throw new RuntimeException("Erro ao enviar mensagem para o kafka", ex);
        }

    }

    private boolean isValidShop(ShopItemDTO item, Product product) {

        return product != null && product.getAmount() >= item.getAmount();

    }

}
