package com.jpmc.midascore.listener;

import com.jpmc.midascore.entity.TransactionRecord;
import com.jpmc.midascore.entity.UserRecord;
import com.jpmc.midascore.repository.TransactionRepository;
import com.jpmc.midascore.repository.UserRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional; // Import Transactional

import java.util.Optional;

@Component
public class TransactionListener {

    private static final Logger LOG = LoggerFactory.getLogger(TransactionListener.class);

    // Inject the repositories
    @Autowired
    private UserRepository userRepository;

    @Autowired
    private TransactionRepository transactionRepository;

    /**
     * Listens for Kafka messages (as Strings), parses them, validates,
     * and processes valid transactions.
     * @param message Raw message string (e.g., "senderId,recipientId,amount")
     */
    @KafkaListener(topics = "${general.kafka-topic}", groupId = "midas-core-consumer")
    public void receiveTransactionAsString(String message) {
        LOG.debug("<<<<< RAW KAFKA MESSAGE RECEIVED: {} >>>>>", message);
        try {
            String[] parts = message.split(",");
            if (parts.length >= 3) {
                // Parse message parts (assuming IDs are Long)
                Long senderId = Long.parseLong(parts[0].trim());
                Long recipientId = Long.parseLong(parts[1].trim());
                double amount = Double.parseDouble(parts[2].trim());

                // Process the transaction within a database transaction
                processTransaction(senderId, recipientId, amount);

            } else {
                LOG.warn("Invalid message format received (not enough parts): {}", message);
            }
        } catch (NumberFormatException e) {
            LOG.error("Error parsing message parts (non-numeric): '{}'", message, e);
        } catch (Exception e) {
            // Catch any other unexpected errors during initial parsing/processing
            LOG.error("Generic error processing raw message: {}", message, e);
        }
    }

    /**
     * Processes a single transaction, including validation and database updates.
     * This method runs within a database transaction. If any part fails,
     * the whole operation is rolled back automatically by Spring.
     * @param senderId ID of the sender
     * @param recipientId ID of the recipient
     * @param amount Transaction amount
     */
    @Transactional // Ensures atomicity
    public void processTransaction(Long senderId, Long recipientId, double amount) {
        LOG.debug("Processing transaction: SenderID={}, RecipientID={}, Amount={}", senderId, recipientId, amount);

        // Fetch sender and recipient using Optional
        Optional<UserRecord> senderOpt = userRepository.findById(senderId);
        Optional<UserRecord> recipientOpt = userRepository.findById(recipientId);

        // --- Validation ---
        if (senderOpt.isEmpty()) {
            LOG.warn("Transaction discarded: Sender ID {} not found.", senderId);
            return; 
        }
        if (recipientOpt.isEmpty()) {
            LOG.warn("Transaction discarded: Recipient ID {} not found.", recipientId);
            return; 
        }

        UserRecord sender = senderOpt.get();
        UserRecord recipient = recipientOpt.get();

        // Check for non-positive amount
        if (amount <= 0) {
             LOG.warn("Transaction discarded: Amount must be positive (Amount: {}). Sender: {}, Recipient: {}",
                    amount, sender.getName(), recipient.getName());
             return;
        }

        // Check balance
        if (sender.getBalance() < amount) {
            LOG.warn("Transaction discarded: Sender {} has insufficient balance ({} < {}). Recipient: {}",
                    sender.getName(), sender.getBalance(), amount, recipient.getName());
            return; 
        }
        
        // Prevent sending to self
        if (senderId.equals(recipientId)) {
             LOG.warn("Transaction discarded: Sender {} cannot send funds to themselves.", sender.getName());
             return;
        }

        // --- If Valid: Process the transaction ---
        LOG.info("Processing valid transaction: {} -> {} ({})", sender.getName(), recipient.getName(), amount);

        // 1. Update balances
        sender.setBalance(sender.getBalance() - amount);
        recipient.setBalance(recipient.getBalance() + amount);

        // 2. Save updated user records (Spring Data JPA knows these are updates)
        userRepository.save(sender);
        userRepository.save(recipient);

        // 3. Create and save the new transaction record
        TransactionRecord transactionRecord = new TransactionRecord(amount, sender, recipient);
        transactionRepository.save(transactionRecord);

        LOG.debug("Transaction processed and recorded successfully. New balances: {} = {}, {} = {}",
                sender.getName(), sender.getBalance(), recipient.getName(), recipient.getBalance());
    }
}