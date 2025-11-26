package com.store.validator.repository;

import com.store.validator.model.Product;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

@Repository
public interface ProductRepository extends JpaRepository<Product, Long> {

    Product findByProductIdentifier(String productIdentifier);

}
