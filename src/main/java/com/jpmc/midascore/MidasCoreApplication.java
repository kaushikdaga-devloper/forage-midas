package com.jpmc.midascore;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.web.client.RestTemplate;

@SpringBootApplication
public class MidasCoreApplication {

    public static void main(String[] args) {
        SpringApplication.run(MidasCoreApplication.class, args);
    }

    // --- ADD THIS METHOD ---
    /**
     * Creates a RestTemplate bean to be used for making HTTP requests.
     * Spring manages this bean's lifecycle and allows it to be injected elsewhere.
     * @return A configured RestTemplate instance.
     */
    @Bean
    public RestTemplate restTemplate() {
        return new RestTemplate();
    }
    // ---------------------

}
