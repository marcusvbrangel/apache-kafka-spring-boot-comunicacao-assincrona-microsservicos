package com.store.shop.events;

import com.store.shop.dto.ShopDTO;
import com.store.shop.repository.ReportRepository;
import jakarta.transaction.Transactional;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Service;

@Slf4j
@Service
public class ReceiveKafkaMessage {

    private final ReportRepository repository;

    private final static String SHOP_TOPIC_EVENT_NAME = "SHOP_TOPIC_EVENT";

    public ReceiveKafkaMessage(ReportRepository repository) {
        this.repository = repository;
    }

    @Transactional
    @KafkaListener(topics = SHOP_TOPIC_EVENT_NAME, groupId = "report-update-report-group")
    public void listenShopTopic(ShopDTO shopDTO, Acknowledgment acknowledgment) {

        try {

            log.info("Shop Report - Compra rcebida no tópico: {}", shopDTO.getIdentifier());

            repository.incrementShopStatus(shopDTO.getStatus());

            acknowledgment.acknowledge();

        } catch (Exception ex) {

            log.error("Shop Report - Erro no processamento da mensagem: ", ex);

        }

    }

}
