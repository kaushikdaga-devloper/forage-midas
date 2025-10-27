package com.jpmc.midascore.repository;

import com.jpmc.midascore.entity.UserRecord;
import org.springframework.data.jpa.repository.JpaRepository; // Use JpaRepository
import org.springframework.stereotype.Repository;

import java.util.Optional; // Use Optional for return types

@Repository
// Extend JpaRepository for richer functionality
public interface UserRepository extends JpaRepository<UserRecord, Long> { 

    // JpaRepository automatically provides Optional<UserRecord> findById(Long id);
    // No need to declare it here unless you want a custom query.

    // Add this method to find users by name (useful for "waldorf")
    Optional<UserRecord> findByName(String name); 
}