package com.store.shop.events;

import com.store.shop.dto.ShopDTO;
import com.store.shop.dto.ShopItemDTO;
import com.store.shop.model.Shop;
import com.store.shop.repository.ShopRepository;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.test.context.junit.jupiter.SpringExtension;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

@ExtendWith(SpringExtension.class)
class ReceiveKafkaMessageTest {

    @InjectMocks
    private ReceiveKafkaMessage receiveKafkaMessage;

    @Mock
    private ShopRepository shopRepository;

    @Mock
    private Acknowledgment acknowledgment;

    @Test
    public void testSuccessMessageReceived() {

        var shopDTO = new ShopDTO();
        shopDTO.setStatus("SUCCESS");
        shopDTO.setIdentifier("shop-123");

        ShopItemDTO shopItemDTO = new ShopItemDTO();
        shopItemDTO.setAmount(1000);
        shopItemDTO.setProductIdentifier("product-1");
        shopItemDTO.setPrice((float) 100);

        shopDTO.getItems().add(shopItemDTO);

        Shop shop = Shop.convert(shopDTO);

        Mockito
           .when(shopRepository.findByIdentifier(shopDTO.getIdentifier()))
           .thenReturn(shop);

        receiveKafkaMessage.listenShopEvents(shopDTO, acknowledgment);

        // Verifica se findByIdentifier foi chamado com o ID correto
        Mockito
           .verify(shopRepository, Mockito.times(1))
           .findByIdentifier(shopDTO.getIdentifier());

        // Captura o argumento Shop que foi salvo
        ArgumentCaptor<Shop> shopCaptor = ArgumentCaptor.forClass(Shop.class);
        Mockito
           .verify(shopRepository, Mockito.times(1))
           .save(shopCaptor.capture());

        // Valida que o status foi atualizado corretamente
        Shop savedShop = shopCaptor.getValue();
        assertNotNull(savedShop);
        assertEquals("SUCCESS", savedShop.getStatus());

        // Verifica se o acknowledge foi chamado
        Mockito.verify(acknowledgment, Mockito.times(1)).acknowledge();
    }

    @Test
    public void testErrorMessageReceived() {

        var shopDTO = new ShopDTO();
        shopDTO.setStatus("FAILED");
        shopDTO.setIdentifier("shop-456");

        // Simula uma exceção ao buscar a compra
        Mockito
           .when(shopRepository.findByIdentifier(shopDTO.getIdentifier()))
           .thenThrow(new RuntimeException("Compra não encontrada"));

        // O método deve capturar a exceção e não relançá-la
        receiveKafkaMessage.listenShopEvents(shopDTO, acknowledgment);

        // Verifica que findByIdentifier foi chamado
        Mockito
           .verify(shopRepository, Mockito.times(1))
           .findByIdentifier(shopDTO.getIdentifier());

        // Verifica que save NÃO foi chamado (pois houve erro)
        Mockito
           .verify(shopRepository, Mockito.times(0))
           .save(Mockito.any());

        // Verifica que acknowledge NÃO foi chamado (pois houve erro)
        Mockito
           .verify(acknowledgment, Mockito.times(0))
           .acknowledge();
    }


















}