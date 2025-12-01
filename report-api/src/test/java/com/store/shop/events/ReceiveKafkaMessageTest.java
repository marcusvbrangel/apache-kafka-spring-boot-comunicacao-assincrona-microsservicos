package com.store.shop.events;

import com.store.shop.dto.ShopDTO;
import com.store.shop.repository.ReportRepository;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.test.context.junit.jupiter.SpringExtension;

@ExtendWith(SpringExtension.class)
class ReceiveKafkaMessageTest {

    @InjectMocks
    private ReceiveKafkaMessage receiveKafkaMessage;

    @Mock
    private ReportRepository reportRepository;

    @Mock
    private Acknowledgment acknowledgment;

    public ShopDTO getShopDTO() {
        ShopDTO shopDTO = new ShopDTO();
        shopDTO.setIdentifier("shop-123");
        shopDTO.setStatus("SUCCESS");
        return shopDTO;
    }

    @Test
    public void testProcessShopSuccess() {

        ShopDTO shopDTO = getShopDTO();

        receiveKafkaMessage.listenShopTopic(shopDTO, acknowledgment);

        // Verifica se incrementShopStatus foi chamado com o status correto
        Mockito
                .verify(reportRepository, Mockito.times(1))
                .incrementShopStatus("SUCCESS");

        // Verifica se acknowledge foi chamado
        Mockito.verify(acknowledgment, Mockito.times(1)).acknowledge();
    }

    @Test
    public void testProcessShopError() {

        ShopDTO shopDTO = getShopDTO();
        shopDTO.setStatus("ERROR");

        receiveKafkaMessage.listenShopTopic(shopDTO, acknowledgment);

        // Verifica se incrementShopStatus foi chamado com o status ERROR
        Mockito
                .verify(reportRepository, Mockito.times(1))
                .incrementShopStatus("ERROR");

        // Verifica se acknowledge foi chamado
        Mockito.verify(acknowledgment, Mockito.times(1)).acknowledge();
    }

    @Test
    public void testProcessShopException() {

        ShopDTO shopDTO = getShopDTO();

        // Simula uma exceção ao tentar incrementar o status
        Mockito
                .doThrow(new RuntimeException("Erro ao atualizar relatório"))
                .when(reportRepository)
                .incrementShopStatus(shopDTO.getStatus());

        // O método deve capturar a exceção e não relançá-la
        receiveKafkaMessage.listenShopTopic(shopDTO, acknowledgment);

        // Verifica que incrementShopStatus foi chamado
        Mockito
                .verify(reportRepository, Mockito.times(1))
                .incrementShopStatus("SUCCESS");

        // Verifica que acknowledge NÃO foi chamado (pois houve erro)
        Mockito
                .verify(acknowledgment, Mockito.times(0))
                .acknowledge();
    }

}