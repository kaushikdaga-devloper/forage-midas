package com.jpmc.midascore.dto;

// This class doesn't need JPA annotations because it's just for holding data from the API call,
// not for storing in our database.
public class IncentiveResponse {

    // Must match the field name in the JSON response ("amount")
    private float amount; 

    // Default constructor (needed by JSON deserializers like Jackson)
    public IncentiveResponse() {
    }

    // --- Getter ---
    // Allows us to retrieve the amount after deserialization
    public float getAmount() {
        return amount;
    }

    // --- Setter ---
    // Allows the JSON deserializer to set the amount
    public void setAmount(float amount) {
        this.amount = amount;
    }

    // Optional: toString() for logging
    @Override
    public String toString() {
        return "IncentiveResponse{" +
               "amount=" + amount +
               '}';
    }
}