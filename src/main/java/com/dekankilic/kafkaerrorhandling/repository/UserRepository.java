package com.dekankilic.kafkaerrorhandling.repository;

import com.dekankilic.kafkaerrorhandling.model.User;
import org.springframework.data.jpa.repository.JpaRepository;

public interface UserRepository extends JpaRepository<User, Long> {
}
