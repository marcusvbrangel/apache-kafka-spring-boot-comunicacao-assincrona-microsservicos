package com.store.shop.events;

import com.store.shop.dto.ShopDTO;
import com.store.shop.dto.ShopItemDTO;
import com.store.shop.model.Product;
import com.store.shop.repository.ProductRepository;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
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

    @KafkaListener(topics = SHOP_TOPIC_NAME, groupId = "group")
    public void listenShopTopic(ShopDTO shopDTO) {

        try {

            log.info("Compra recebida no tópico: {}", shopDTO.getIdentifier());

            boolean success = true;

            for (ShopItemDTO item : shopDTO.getItems()) {

                Product product = productRepository.findByIdentifier(item.getProductIdentifier());

                if (!isValidShop(item, product)) {
                    shopError(shopDTO);
                    success = false;
                    break;
                }

            }

            if (success) {
                shopSuccess(shopDTO);
            }

        } catch (Exception ex) {
            log.error("Erro no processamento da compra: {}", shopDTO.getIdentifier());
        }

    }

    private void shopSuccess(ShopDTO shopDTO) {

        log.info("Compra efetuada com sucesso: {}", shopDTO.getIdentifier());

        shopDTO.setStatus(SHOP_STATUS_SUCCESS);

        kafkaTemplate.send(SHOP_TOPIC_EVENT_NAME, shopDTO);

    }

    private void shopError(ShopDTO shopDTO) {

        log.info("Erro no processamento da compra: {}", shopDTO.getIdentifier());

        shopDTO.setStatus(SHOP_STATUS_ERROR);

        kafkaTemplate.send(SHOP_TOPIC_EVENT_NAME, shopDTO);

    }

    private boolean isValidShop(ShopItemDTO item, Product product) {

        return product != null && product.getAmount() >= item.getAmount();

    }

}
