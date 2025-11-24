package com.store.shop_api.controller;

import com.store.shop_api.constants.ShopStatus;
import com.store.shop_api.dto.ShopDTO;
import com.store.shop_api.model.Shop;
import com.store.shop_api.repository.ShopRepository;
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

    public ShopController(ShopRepository shopRepository) {
        this.shopRepository = shopRepository;
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
