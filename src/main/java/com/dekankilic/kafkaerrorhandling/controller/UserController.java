package com.dekankilic.kafkaerrorhandling.controller;

import com.dekankilic.kafkaerrorhandling.model.User;
import com.dekankilic.kafkaerrorhandling.service.UserService;
import lombok.RequiredArgsConstructor;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/users")
@RequiredArgsConstructor
public class UserController {
    private final UserService userService;

    @PostMapping("/publishNew")
    public ResponseEntity<String> publishEvent(@RequestBody User user){
        userService.sendEvents(user);
        return ResponseEntity
                .status(HttpStatus.OK)
                .body("Message published successfully");
    }
}
