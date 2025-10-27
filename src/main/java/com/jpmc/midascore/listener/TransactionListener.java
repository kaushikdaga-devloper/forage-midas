package com.jpmc.midascore.listener;

import com.jpmc.midascore.dto.IncentiveResponse; // <-- Add DTO import
import com.jpmc.midascore.entity.TransactionRecord;
import com.jpmc.midascore.entity.UserRecord;
import com.jpmc.midascore.foundation.Transaction; // <-- Add foundation Transaction import
import com.jpmc.midascore.repository.TransactionRepository;
import com.jpmc.midascore.repository.UserRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.web.client.RestClientException; // <-- Add Exception import
import org.springframework.web.client.RestTemplate;     // <-- Add RestTemplate import

import java.util.Optional;

@Component
public class TransactionListener {

    private static final Logger LOG = LoggerFactory.getLogger(TransactionListener.class);

    @Autowired
    private UserRepository userRepository;

    @Autowired
    private TransactionRepository transactionRepository;

    // --- ADD RestTemplate Injection ---
    @Autowired
    private RestTemplate restTemplate;
    // ----------------------------------

    // Define the Incentive API URL (could be moved to application.yml later)
    private final String incentiveApiUrl = "http://localhost:8080/incentive";

    @KafkaListener(topics = "${general.kafka-topic}", groupId = "midas-core-consumer")
    public void receiveTransactionAsString(String message) {
        LOG.debug("<<<<< RAW KAFKA MESSAGE RECEIVED: {} >>>>>", message);
        try {
            String[] parts = message.split(",");
            if (parts.length >= 3) {
                Long senderId = Long.parseLong(parts[0].trim());
                Long recipientId = Long.parseLong(parts[1].trim());
                float amount = Float.parseFloat(parts[2].trim());

                processTransaction(senderId, recipientId, amount);

            } else {
                LOG.warn("Invalid message format received (not enough parts): {}", message);
            }
        } catch (NumberFormatException e) {
            LOG.error("Error parsing message parts (non-numeric): '{}'", message, e);
        } catch (Exception e) {
            LOG.error("Generic error processing raw message: {}", message, e);
        }
    }

    @Transactional
    public void processTransaction(Long senderId, Long recipientId, float amount) {
        LOG.debug("Processing transaction: SenderID={}, RecipientID={}, Amount={}", senderId, recipientId, amount);

        Optional<UserRecord> senderOpt = userRepository.findById(senderId);
        Optional<UserRecord> recipientOpt = userRepository.findById(recipientId);

        // --- Basic Validation ---
        if (senderOpt.isEmpty() || recipientOpt.isEmpty()) {
            LOG.warn("Transaction discarded: Sender ({}) or Recipient ({}) not found.", senderId, recipientId);
            return;
        }

        UserRecord sender = senderOpt.get();
        UserRecord recipient = recipientOpt.get();

        if (amount <= 0) {
             LOG.warn("Transaction discarded: Amount must be positive. Sender: {}, Recipient: {}, Amount: {}",
                    sender.getName(), recipient.getName(), amount);
             return;
        }

        if (sender.getBalance() < amount) {
            LOG.warn("Transaction discarded: Sender {} has insufficient balance ({} < {}). Recipient: {}",
                    sender.getName(), sender.getBalance(), amount, recipient.getName());
            return;
        }
        
        if (senderId.equals(recipientId)) {
             LOG.warn("Transaction discarded: Sender {} cannot send funds to themselves.", sender.getName());
             return;
        }

        // --- Call Incentive API ---
        float incentiveAmount = 0.0f; // Default incentive
        try {
            // Create the Transaction object required by the API
            Transaction transactionForApi = new Transaction(senderId, recipientId, amount);
            LOG.debug("Calling Incentive API: {}", transactionForApi);
            
            // Make the POST request and get the response
            IncentiveResponse incentiveResponse = restTemplate.postForObject(incentiveApiUrl, transactionForApi, IncentiveResponse.class);

            if (incentiveResponse != null && incentiveResponse.getAmount() >= 0) {
                incentiveAmount = incentiveResponse.getAmount();
                LOG.info("Received incentive: {}", incentiveAmount);
            } else {
                 LOG.warn("Received null or invalid incentive response: {}", incentiveResponse);
            }
        } catch (RestClientException e) {
            // Log error if API call fails, but proceed with 0 incentive
            LOG.error("Error calling Incentive API for transaction SenderID={}, RecipientID={}, Amount={}. Proceeding with 0 incentive.",
                      senderId, recipientId, amount, e);
            // Optionally, you might decide to fail the transaction here depending on requirements
        }
        // ------------------------


        // --- If Valid: Process the transaction ---
        LOG.info("Processing valid transaction: {} -> {} (Amount: {}, Incentive: {})", 
                 sender.getName(), recipient.getName(), amount, incentiveAmount);

        // 1. Update balances (Recipient gets amount + incentive)
        sender.setBalance(sender.getBalance() - amount);
        recipient.setBalance(recipient.getBalance() + amount + incentiveAmount); // Add incentive here

        // 2. Save updated user records
        userRepository.save(sender);
        userRepository.save(recipient);

        // 3. Create and save the transaction record (including incentive)
        TransactionRecord transactionRecord = new TransactionRecord(amount, incentiveAmount, sender, recipient); // Pass incentiveAmount
        transactionRepository.save(transactionRecord);

        LOG.debug("Transaction processed and recorded successfully. Incentive: {}. New balances: {} = {}, {} = {}",
                incentiveAmount, sender.getName(), sender.getBalance(), recipient.getName(), recipient.getBalance());
    }
}