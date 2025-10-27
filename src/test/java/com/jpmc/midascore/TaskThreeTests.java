package com.jpmc.midascore;

// Import UserRepository
import com.jpmc.midascore.repository.UserRepository; 
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.test.annotation.DirtiesContext;

@SpringBootTest
@DirtiesContext
@EmbeddedKafka(partitions = 1, brokerProperties = {"listeners=PLAINTEXT://localhost:9092", "port=9092"})
public class TaskThreeTests {
    static final Logger logger = LoggerFactory.getLogger(TaskThreeTests.class);

    @Autowired
    private KafkaProducer kafkaProducer;

    @Autowired
    private UserPopulator userPopulator;

    @Autowired
    private FileLoader fileLoader;

    // --- ADDED FIELD ---
    @Autowired 
    private UserRepository userRepository; 
    // -------------------

    @Test
    void task_three_verifier() throws InterruptedException {
        // Populate initial users and balances
        userPopulator.populate(); 

        // Load and send transaction messages
        String[] transactionLines = fileLoader.loadStrings("/test_data/mnbvcxz.vbnm");
        for (String transactionLine : transactionLines) {
            kafkaProducer.send(transactionLine);
        }

        // Wait for listener to process messages (Increased from 2000)
        Thread.sleep(5000); // Wait 5 seconds just to be safer 

        // --- ADDED LOGGING ---
        // Find Waldorf and print the balance before the loop
        userRepository.findByName("waldorf").ifPresent(user -> {
            logger.info(">>>>>>>>>> WALDORF FINAL BALANCE: {} <<<<<<<<<<", user.getBalance());
        });
        // ---------------------

        logger.info("----------------------------------------------------------");
        logger.info("use your debugger to find out what waldorf's balance is after all transactions are processed");
        logger.info("kill this test once you find the answer");
        
        // Infinite loop (test keeps running until stopped manually)
        while (true) { 
            Thread.sleep(20000);
            logger.info("...");
        }
    }
}