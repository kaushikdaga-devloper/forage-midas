package com.jpmc.midascore.entity;

import jakarta.persistence.Entity;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.GenerationType;
import jakarta.persistence.Id;
import jakarta.persistence.JoinColumn;
import jakarta.persistence.ManyToOne;

// Marks this class as a JPA entity, meaning it corresponds to a database table
@Entity 
public class TransactionRecord {

    // Marks 'id' as the primary key and configures auto-generation
    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY) 
    private Long id;

    // Stores the amount of the transaction
    private double amount;

    // Establishes a many-to-one relationship with UserRecord for the sender
    // Many transactions can have the same sender user.
    @ManyToOne 
    @JoinColumn(name = "sender_id", nullable = false) // Defines the foreign key column in the transaction_record table
    private UserRecord sender;

    // Establishes a many-to-one relationship with UserRecord for the recipient
    @ManyToOne 
    @JoinColumn(name = "recipient_id", nullable = false) // Defines the foreign key column
    private UserRecord recipient;

    // --- Constructors ---
    
    // Default constructor (required by JPA)
    public TransactionRecord() {
    }

    // Constructor for creating new records
    public TransactionRecord(double amount, UserRecord sender, UserRecord recipient) {
        this.amount = amount;
        this.sender = sender;
        this.recipient = recipient;
    }

    // --- Getters and Setters --- 
    // (Required by JPA and useful for accessing/modifying data)

    public Long getId() {
        return id;
    }

    public void setId(Long id) {
        this.id = id;
    }

    public double getAmount() {
        return amount;
    }

    public void setAmount(double amount) {
        this.amount = amount;
    }

    public UserRecord getSender() {
        return sender;
    }

    public void setSender(UserRecord sender) {
        this.sender = sender;
    }

    public UserRecord getRecipient() {
        return recipient;
    }

    public void setRecipient(UserRecord recipient) {
        this.recipient = recipient;
    }

    // Optional: toString() method for logging/debugging
    @Override
    public String toString() {
        return "TransactionRecord{" +
                "id=" + id +
                ", amount=" + amount +
                ", sender=" + (sender != null ? sender.getName() : "null") + // Avoid NullPointerException
                ", recipient=" + (recipient != null ? recipient.getName() : "null") + // Avoid NullPointerException
                '}';
    }
}
