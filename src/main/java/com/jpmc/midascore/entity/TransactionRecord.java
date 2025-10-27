package com.jpmc.midascore.entity;

import jakarta.persistence.Entity;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.GenerationType;
import jakarta.persistence.Id;
import jakarta.persistence.JoinColumn;
import jakarta.persistence.ManyToOne;

@Entity
public class TransactionRecord {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    // Keep original amount
    private float amount;

    // --- ADD THIS FIELD ---
    // Stores the incentive amount obtained from the API
    private float incentiveAmount;
    // --------------------

    @ManyToOne
    @JoinColumn(name = "sender_id", nullable = false)
    private UserRecord sender;

    @ManyToOne
    @JoinColumn(name = "recipient_id", nullable = false)
    private UserRecord recipient;

    // --- Constructors ---

    // Default constructor (required by JPA)
    public TransactionRecord() {
    }

    // --- MODIFY Constructor to include incentiveAmount ---
    public TransactionRecord(float amount, float incentiveAmount, UserRecord sender, UserRecord recipient) {
        this.amount = amount;
        this.incentiveAmount = incentiveAmount; // Set the new field
        this.sender = sender;
        this.recipient = recipient;
    }
    // ----------------------------------------------------

    // --- Getters and Setters ---
    // (Keep existing ones)

    public Long getId() {
        return id;
    }

    public void setId(Long id) {
        this.id = id;
    }

    public float getAmount() {
        return amount;
    }

    public void setAmount(float amount) {
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

    // --- ADD Getter and Setter for incentiveAmount ---
    public float getIncentiveAmount() {
        return incentiveAmount;
    }

    public void setIncentiveAmount(float incentiveAmount) {
        this.incentiveAmount = incentiveAmount;
    }
    // ------------------------------------------------

    // Optional: toString() updated
    @Override
    public String toString() {
        return "TransactionRecord{" +
                "id=" + id +
                ", amount=" + amount +
                ", incentiveAmount=" + incentiveAmount + // Include new field
                ", sender=" + (sender != null ? sender.getName() : "null") +
                ", recipient=" + (recipient != null ? recipient.getName() : "null") +
                '}';
    }
}