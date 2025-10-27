package com.jpmc.midascore.listener;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
public class TransactionListener {

    private static final Logger LOG = LoggerFactory.getLogger(TransactionListener.class);

    public TransactionListener() {
        LOG.info("<<<<< TransactionListener Bean CREATED >>>>>");
    }

    // Keep accepting String for Task 2 test compatibility
    @KafkaListener(topics = "${general.kafka-topic}", groupId = "midas-core-consumer") // Explicitly add groupId here
    public void receiveTransactionAsString(String message) { 
        
        LOG.debug("<<<<< RAW KAFKA MESSAGE RECEIVED: {} >>>>>", message); 

        // --- Corrected Parsing Logic ---
        try {
            String[] parts = message.split(","); 
            if (parts.length > 2) { 
                double amount = Double.parseDouble(parts[2].trim()); 
                // Use INFO level for the specific output needed
                LOG.info(">>> TASK 2 EXTRACTED AMOUNT: {} <<<", amount); 
            } else {
                LOG.warn("Could not parse amount from message (not enough parts): {}", message);
            }
        } catch (Exception e) {
            LOG.error("Error processing message: {}", message, e);
        }
    }
}