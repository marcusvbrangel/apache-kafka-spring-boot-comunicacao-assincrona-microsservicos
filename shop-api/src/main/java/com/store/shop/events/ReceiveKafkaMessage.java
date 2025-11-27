package com.store.shop.events;

import com.store.shop.dto.ShopDTO;
import com.store.shop.model.Shop;
import com.store.shop.repository.ShopRepository;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

@Slf4j
@Service
public class ReceiveKafkaMessage {

    private final ShopRepository shopRepository;

    private static final String SHOP_TOPIC_EVENT_NAME = "SHOP_TOPIC_EVENT";

    public ReceiveKafkaMessage(ShopRepository shopRepository) {
        this.shopRepository = shopRepository;
    }

    @KafkaListener(topics = SHOP_TOPIC_EVENT_NAME, groupId = "group")
    public void listenShopEvents(ShopDTO shopDTO) {

        try {

            log.info("Status da compra recebida no tópico: {}", shopDTO.getIdentifier());

            Shop shop = shopRepository.findByIdentifier(shopDTO.getIdentifier());

            shop.setStatus(shopDTO.getStatus());

            shopRepository.save(shop);

        } catch (Exception ex) {

            log.error("Erro no processamento da compra: {}", shopDTO.getIdentifier());

        }

    }

}
