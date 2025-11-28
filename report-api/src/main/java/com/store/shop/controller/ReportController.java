package com.store.shop.controller;

import com.store.shop.dto.ShopReportDTO;
import com.store.shop.repository.ReportRepository;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseStatus;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;

@RestController
@RequestMapping("/shop/report")
public class ReportController {

    private final ReportRepository reportRepository;

    public ReportController(ReportRepository reportRepository) {
        this.reportRepository = reportRepository;
    }

    @GetMapping
    @ResponseStatus(HttpStatus.OK)
    public ResponseEntity<List<ShopReportDTO>> getAll() {

        List<ShopReportDTO> list = reportRepository.findAll()
                .stream()
                .map(shop -> ShopReportDTO.convert(shop))
                .toList();

        return ResponseEntity.ok(list);

    }

}
