package com.store.shopapi.dto;

import com.store.shopapi.model.Shop;
import lombok.Getter;
import lombok.Setter;

import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;

@Getter
@Setter
public class ShopDTO {

    private String identifier;
    private LocalDateTime dateShop;
    private String status;
    private String buyerIdentifier;
    private List<ShopItemDTO> items = new  ArrayList<>();

    public static ShopDTO convert(Shop shop) {
        ShopDTO shopDTO = new ShopDTO();
        shopDTO.setIdentifier(shop.getIdentifier());
        shopDTO.setDateShop(shop.getDateShop());
        shopDTO.setStatus(shop.getStatus());
        shopDTO.setBuyerIdentifier(shop.getBuyerIdentifier());
        shopDTO.setItems(shop.getItems()
                .stream()
                .map(item -> ShopItemDTO.convert(item))
                .toList());
        return shopDTO;
    }

}