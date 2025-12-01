package com.store.shop.events;

import com.store.shop.dto.ShopDTO;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.Message;
import org.springframework.test.context.junit.jupiter.SpringExtension;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

@ExtendWith(SpringExtension.class)
class SendKafkaMessageTest {

    @InjectMocks
    private SendKafkaMessage sendKafkaMessage;

    @Mock
    private KafkaTemplate<String, ShopDTO> kafkaTemplate;

    private static final String SHOP_TOPIC_NAME = "SHOP_TOPIC";

    @Test
    public void testSendMessage() {

        var shopDTO = new ShopDTO();
        shopDTO.setStatus("SUCCESS");
        shopDTO.setBuyerIdentifier("b-1");

        sendKafkaMessage.sendMessage(shopDTO);

        // Captura o argumento Message que foi enviado
        @SuppressWarnings("unchecked")
        ArgumentCaptor<Message<ShopDTO>> messageCaptor = ArgumentCaptor.forClass(Message.class);
        Mockito.verify(kafkaTemplate, Mockito.times(1)).send(messageCaptor.capture());

        // Valida a mensagem capturada
        Message<ShopDTO> capturedMessage = messageCaptor.getValue();
        assertNotNull(capturedMessage);
        assertEquals(shopDTO, capturedMessage.getPayload());
        assertEquals("b-1", capturedMessage.getHeaders().get(KafkaHeaders.KEY));
        assertEquals(SHOP_TOPIC_NAME, capturedMessage.getHeaders().get(KafkaHeaders.TOPIC));
        assertEquals("shop-api", capturedMessage.getHeaders().get("source"));
        assertNotNull(capturedMessage.getHeaders().get(KafkaHeaders.TIMESTAMP));

    }

}