package com.dekankilic.kafkaerrorhandling.service;


import com.dekankilic.kafkaerrorhandling.model.User;
import com.dekankilic.kafkaerrorhandling.repository.UserRepository;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.DltHandler;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.annotation.RetryableTopic;
import org.springframework.kafka.support.SendResult;
import lombok.RequiredArgsConstructor;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.retry.annotation.Backoff;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@Service
@RequiredArgsConstructor
@Slf4j
public class UserService {
    private final UserRepository userRepository;
    private final KafkaTemplate kafkaTemplate;

    @Transactional
    public void sendEvents(User user){
        try{
            userRepository.save(user);

            CompletableFuture<SendResult<String, Object>> future = kafkaTemplate.send("kafka-error-handle", user);
            future.whenComplete((result, ex) -> {
                if(ex == null){
                    log.info("Sent message: {}", user);
                }else{
                    System.out.println("Unable to send message=[" +
                            user.toString() + "] due to : " + ex.getMessage());
                }
            });
        }catch (Exception ex){
            System.out.println(ex.getMessage());
        }
    }

    // 3 topics will be created with the suffix retry, and each of them will retry once.
    // backoff means if you do not want to perform retry one after another immediately.
    // exlude means for which exceptions you do not want to perform retry.
    @RetryableTopic(attempts = "4", backoff = @Backoff(delay = 3000, multiplier = 1.5, maxDelay = 15000), exclude = {NullPointerException.class})
    @KafkaListener(topics = "kafka-error-handle", groupId = "example-group")
    public void consumeEvent(User user) {
        try{
            log.info("Received: {} from kafka-error-handle ", new ObjectMapper().writeValueAsString(user));
            // validate restricted IP before processing the records
            List<String> restrictedIps = Stream.of("32.241.244.236", "15.55.49.164", "81.1.95.253", "126.130.43.183").collect(Collectors.toList());
            if(restrictedIps.contains(user.getIpAddress())){
                throw new RuntimeException("Invalid IP Address received!");
            }
        } catch (JsonProcessingException ex){
            ex.printStackTrace();
        }
    }

    @DltHandler
    public void listedDLT(User user){
        log.info("DLT Received : {}", user.getFirstName());
    }

}
