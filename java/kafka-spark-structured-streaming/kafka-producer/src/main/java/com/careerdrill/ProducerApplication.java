package com.careerdrill;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.boot.ApplicationRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.util.concurrent.ListenableFuture;
import org.springframework.util.concurrent.ListenableFutureCallback;

@SpringBootApplication
public class ProducerApplication {
    private static String days[] = {"SUNDAY","MONDAY", "TUESDAY", "WEDNESDAY", "THURSDAY","FRIDAY", "SATURDAY"};
    private static final int MAX_RANDOM_DAYS = 50;

    public static void main(String[] args) {
        SpringApplication.run(ProducerApplication.class, args);
    }

    @Bean
    public NewTopic topic() {
        return TopicBuilder.name("topic1")
                .partitions(10)
                .replicas(1)
                .build();
    }



    @Bean
    public ApplicationRunner runner(KafkaTemplate<String, String> template) {
        return args -> {

            int randomIndex = 0;

            for (int i = 0; i < MAX_RANDOM_DAYS; i++) {
                randomIndex = (int) (Math.random() * days.length);
                ListenableFuture<SendResult<String, String>> future = template.send("topic1", days[randomIndex]);

                future.addCallback(new ListenableFutureCallback<>() {

                    @Override
                    public void onSuccess(SendResult<String, String> result) {
                        System.out.println("Success");
                    }

                    @Override
                    public void onFailure(Throwable ex) {
                        System.out.println("Failure");
                    }

                });


            }

        };
    }

}