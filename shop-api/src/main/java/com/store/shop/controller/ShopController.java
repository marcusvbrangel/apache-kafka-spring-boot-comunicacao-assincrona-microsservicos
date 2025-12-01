package com.store.shop.controller;

import com.store.shop.constants.ShopStatus;
import com.store.shop.dto.ShopDTO;
import com.store.shop.events.SendKafkaMessage;
import com.store.shop.model.Shop;
import com.store.shop.repository.ShopRepository;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.time.LocalDateTime;
import java.util.List;
import java.util.UUID;

@RestController
@RequestMapping("/shop")
public class ShopController {

    private final ShopRepository shopRepository;
    private final SendKafkaMessage kafkaClient;

    public ShopController(ShopRepository shopRepository, SendKafkaMessage kafkaClient) {
        this.shopRepository = shopRepository;
        this.kafkaClient = kafkaClient;
    }

    @GetMapping
    @ResponseStatus(HttpStatus.OK)
    public ResponseEntity<List<ShopDTO>> getAll() {

        List<ShopDTO> list = shopRepository.findAll()
                .stream()
                .map(shop -> ShopDTO.convert(shop))
                .toList();

        return ResponseEntity.ok(list);

    }

    @PostMapping
    @ResponseStatus(HttpStatus.CREATED)
    public ResponseEntity<ShopDTO> saveShop(@RequestBody ShopDTO shopDTO) {

        shopDTO.setIdentifier(UUID.randomUUID().toString());
        shopDTO.setDateShop(LocalDateTime.now());
        shopDTO.setStatus(ShopStatus.PENDING.toString());

        Shop shop = Shop.convert(shopDTO);

        shop.getItems()
                .stream()
                .forEach(shopItem -> shopItem.setShop(shop));

        shopDTO = ShopDTO.convert(shopRepository.save(shop));

        kafkaClient.sendMessage(shopDTO);

        return ResponseEntity.status(HttpStatus.CREATED).body(shopDTO);

    }
/*
    {
        "items": [
        {
            "productIdentifier": "123456789",
                "amount": "100",
                "price": "1000"
        },
        {
            "productIdentifier": "123456789",
                "amount": "100",
                "price": "1000"
        }
]
    }

 */

}
