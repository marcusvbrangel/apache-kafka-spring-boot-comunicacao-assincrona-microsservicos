package com.store.shop.dto;

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

}