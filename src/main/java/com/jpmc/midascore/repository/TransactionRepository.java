package com.jpmc.midascore.repository;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import com.jpmc.midascore.entity.TransactionRecord;

// Marks this interface as a Spring Data repository
@Repository 
public interface TransactionRepository extends JpaRepository<TransactionRecord, Long> {
    // Spring Data JPA automatically provides common database methods like:
    // save(), findById(), findAll(), deleteById(), etc.
    // We don't need to declare them here unless we need custom queries.
}
