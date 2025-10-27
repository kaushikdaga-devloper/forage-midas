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
import org.springframework.transaction.annotation.Transactional;

import java.util.Optional;

@Component
public class TransactionListener {

    private static final Logger LOG = LoggerFactory.getLogger(TransactionListener.class);

    @Autowired
    private UserRepository userRepository;

    @Autowired
    private TransactionRepository transactionRepository;

    @KafkaListener(topics = "${general.kafka-topic}", groupId = "midas-core-consumer")
    public void receiveTransactionAsString(String message) {
        LOG.debug("<<<<< RAW KAFKA MESSAGE RECEIVED: {} >>>>>", message);
        try {
            String[] parts = message.split(",");
            if (parts.length >= 3) {
                Long senderId = Long.parseLong(parts[0].trim());
                Long recipientId = Long.parseLong(parts[1].trim());
                // --- CHANGE TO float ---
                float amount = Float.parseFloat(parts[2].trim()); 
                // ---------------------

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
    // --- CHANGE amount type to float ---
    public void processTransaction(Long senderId, Long recipientId, float amount) { 
    // ---------------------------------
        LOG.debug("Processing transaction: SenderID={}, RecipientID={}, Amount={}", senderId, recipientId, amount);

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

        if (amount <= 0) {
             LOG.warn("Transaction discarded: Amount must be positive (Amount: {}). Sender: {}, Recipient: {}",
                    amount, sender.getName(), recipient.getName());
             return;
        }

        // --- Use float comparison ---
        if (sender.getBalance() < amount) { 
        // --------------------------
            LOG.warn("Transaction discarded: Sender {} has insufficient balance ({} < {}). Recipient: {}",
                    sender.getName(), sender.getBalance(), amount, recipient.getName());
            return; 
        }
        
        if (senderId.equals(recipientId)) {
             LOG.warn("Transaction discarded: Sender {} cannot send funds to themselves.", sender.getName());
             return;
        }

        LOG.info("Processing valid transaction: {} -> {} ({})", sender.getName(), recipient.getName(), amount);

        // --- Calculations now use float ---
        sender.setBalance(sender.getBalance() - amount);
        recipient.setBalance(recipient.getBalance() + amount);
        // --------------------------------

        userRepository.save(sender);
        userRepository.save(recipient);

        // --- Pass float amount to constructor ---
        // (Make sure TransactionRecord's amount field is float or compatible)
        TransactionRecord transactionRecord = new TransactionRecord(amount, sender, recipient); 
        transactionRepository.save(transactionRecord);
        // ----------------------------------------

        LOG.debug("Transaction processed and recorded successfully. New balances: {} = {}, {} = {}",
                sender.getName(), sender.getBalance(), recipient.getName(), recipient.getBalance());
    }
}