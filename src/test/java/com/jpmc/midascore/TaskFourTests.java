package com.jpmc.midascore;

// Add this import
import com.jpmc.midascore.repository.UserRepository; 
// Keep other existing imports
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
public class TaskFourTests {
    static final Logger logger = LoggerFactory.getLogger(TaskFourTests.class);

    @Autowired
    private KafkaProducer kafkaProducer;

    @Autowired
    private UserPopulator userPopulator;

    @Autowired
    private FileLoader fileLoader;

    // --- ADD THIS ---
    @Autowired 
    private UserRepository userRepository; 
    // --------------

    @Test
    void task_four_verifier() throws InterruptedException {
        // Populate users
        userPopulator.populate();

        // Load and send transactions
        String[] transactionLines = fileLoader.loadStrings("/test_data/alskdjfh.fhdjsk"); // Using the correct file for Task 4
        for (String transactionLine : transactionLines) {
            kafkaProducer.send(transactionLine);
        }

        // Wait for Kafka listener to process (Increased for safety)
        Thread.sleep(10000); // Wait 10 seconds 

        // --- ADD THIS BLOCK to find and print Wilbur's balance ---
        logger.info("--- Attempting to find Wilbur's final balance ---");
        userRepository.findByName("wilbur").ifPresentOrElse(
            user -> {
                logger.info(">>>>>>>>>> WILBUR FINAL BALANCE: {} <<<<<<<<<<", user.getBalance());
            },
            () -> {
                logger.error("<<<<<<<<<< WILBUR USER NOT FOUND IN DATABASE >>>>>>>>>>");
            }
        );
        logger.info("--- Finished checking Wilbur's balance ---");
        // --------------------------------------------------------

        // Original logging and infinite loop
        logger.info("----------------------------------------------------------");
        logger.info("use your debugger to figure out the balance of the \\\"wilbur\\\" user..."); // Note: Escaped quotes needed here
        logger.info("kill this test once you find the answer");
        while (true) {
            Thread.sleep(20000);
            logger.info("...");
        }
    }
}