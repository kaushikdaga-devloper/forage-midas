package com.jpmc.midascore;

import com.jpmc.midascore.foundation.Transaction; // Keep this import if needed elsewhere, but not for send method parameter
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

@Component
public class KafkaProducer {
    private static final Logger logger = LoggerFactory.getLogger(KafkaProducer.class);

    @Value("${general.kafka-topic}")
    private String topicName;

    // --- CHANGE KafkaTemplate TO <String, String> ---
    @Autowired
    private KafkaTemplate<String, String> kafkaTemplate; 
    // ----------------------------------------------

    // This method is correctly called by TaskTwoTests
    public void send(String message) {
        logger.info(String.format("#### -> Producing message -> %s", message));
        // Now sends String using a KafkaTemplate specifically for Strings
        this.kafkaTemplate.send(topicName, message); 
    }

    // --- REMOVE OR COMMENT OUT this method ---
    // It causes confusion and is not used by TaskTwoTests
    // public void send(Transaction transaction) { 
    //     logger.info(String.format("#### -> Producing transaction -> %s", transaction.toString()));
    //     this.kafkaTemplate.send(topicName, transaction); // This was likely causing the error indirectly
    // }
    // ----------------------------------------
}