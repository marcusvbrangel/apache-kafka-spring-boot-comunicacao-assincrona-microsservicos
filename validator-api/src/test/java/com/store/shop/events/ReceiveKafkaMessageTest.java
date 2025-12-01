package com.store.shop.events;

import com.store.shop.dto.ShopDTO;
import com.store.shop.dto.ShopItemDTO;
import com.store.shop.model.Product;
import com.store.shop.repository.ProductRepository;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.Message;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.test.context.junit.jupiter.SpringExtension;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

@ExtendWith(SpringExtension.class)
class ReceiveKafkaMessageTest {

    @InjectMocks
    private ReceiveKafkaMessage receiveKafkaMessage;

    @Mock
    private KafkaTemplate<String, ShopDTO> kafkaTemplate;

    @Mock
    private ProductRepository productRepository;

    @Mock
    private Acknowledgment acknowledgment;

    private static final String SHOP_TOPIC_EVENT_NAME = "SHOP_TOPIC_EVENT";

    public ShopDTO getShopDTO() {

        ShopDTO shopDTO = new ShopDTO();
        shopDTO.setIdentifier("shop-123");
        shopDTO.setBuyerIdentifier("b-1");

        ShopItemDTO shopItemDTO = new ShopItemDTO();
        shopItemDTO.setAmount(100);
        shopItemDTO.setProductIdentifier("product-1");
        shopItemDTO.setPrice((float) 100);

        shopDTO.getItems().add(shopItemDTO);

        return shopDTO;
    }

    public Product getProduct() {

        Product product = new Product();
        product.setAmount(1000);
        product.setId(1L);
        product.setIdentifier("product-1");

        return product;

    }

    @Test
    public void testProcessShopSuccess() {

        ShopDTO shopDTO = getShopDTO();
        Product product = getProduct();

        Mockito
                .when(productRepository.findByIdentifier("product-1"))
                .thenReturn(product);

        receiveKafkaMessage.listenShopTopic(shopDTO, acknowledgment, "b-1", "SHOP_TOPIC", 0, System.currentTimeMillis());

        // Captura a mensagem que foi enviada
        @SuppressWarnings("unchecked")
        ArgumentCaptor<Message<ShopDTO>> messageCaptor = ArgumentCaptor.forClass(Message.class);
        Mockito.verify(kafkaTemplate, Mockito.times(1)).send(messageCaptor.capture());

        // Valida a mensagem capturada
        Message<ShopDTO> capturedMessage = messageCaptor.getValue();
        assertNotNull(capturedMessage);
        assertEquals("SUCCESS", capturedMessage.getPayload().getStatus());
        assertEquals("validator-api", capturedMessage.getHeaders().get("source"));
        assertEquals(SHOP_TOPIC_EVENT_NAME, capturedMessage.getHeaders().get(KafkaHeaders.TOPIC));

        // Verifica se acknowledge foi chamado
        Mockito.verify(acknowledgment, Mockito.times(1)).acknowledge();
    }

    @Test
    public void testProcessShopError() {

        ShopDTO shopDTO = getShopDTO();
        Product product = getProduct();

        // Define um produto com quantidade insuficiente (menos que os 100 da compra)
        product.setAmount(50);

        Mockito
                .when(productRepository.findByIdentifier("product-1"))
                .thenReturn(product);

        receiveKafkaMessage.listenShopTopic(shopDTO, acknowledgment, "b-1", "SHOP_TOPIC", 0, System.currentTimeMillis());

        // Captura a mensagem que foi enviada
        @SuppressWarnings("unchecked")
        ArgumentCaptor<Message<ShopDTO>> messageCaptor = ArgumentCaptor.forClass(Message.class);
        Mockito.verify(kafkaTemplate, Mockito.times(1)).send(messageCaptor.capture());

        // Valida a mensagem capturada
        Message<ShopDTO> capturedMessage = messageCaptor.getValue();
        assertNotNull(capturedMessage);
        assertEquals("ERROR", capturedMessage.getPayload().getStatus());
        assertEquals("validator-api", capturedMessage.getHeaders().get("source"));
        assertEquals(SHOP_TOPIC_EVENT_NAME, capturedMessage.getHeaders().get(KafkaHeaders.TOPIC));

        // Verifica se acknowledge foi chamado
        Mockito.verify(acknowledgment, Mockito.times(1)).acknowledge();
    }

    @Test
    public void testProcessShopProductNotFound() {

        ShopDTO shopDTO = getShopDTO();

        Mockito
                .when(productRepository.findByIdentifier("product-1"))
                .thenReturn(null);

        receiveKafkaMessage.listenShopTopic(shopDTO, acknowledgment, "b-1", "SHOP_TOPIC", 0, System.currentTimeMillis());

        // Captura a mensagem que foi enviada
        @SuppressWarnings("unchecked")
        ArgumentCaptor<Message<ShopDTO>> messageCaptor = ArgumentCaptor.forClass(Message.class);
        Mockito.verify(kafkaTemplate, Mockito.times(1)).send(messageCaptor.capture());

        // Valida que o status é ERROR quando produto não encontrado
        Message<ShopDTO> capturedMessage = messageCaptor.getValue();
        assertNotNull(capturedMessage);
        assertEquals("ERROR", capturedMessage.getPayload().getStatus());

        // Verifica se acknowledge foi chamado
        Mockito.verify(acknowledgment, Mockito.times(1)).acknowledge();
    }

}