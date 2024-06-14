package com.study.opensearch.domain;

import jakarta.persistence.*;
import lombok.Builder;
import lombok.Getter;

import java.time.LocalDateTime;
import java.util.UUID;

@Entity
@Table(name = "application_log_entity")
@Getter
public class ApplicationLogEntity {

    @Id
    @GeneratedValue(strategy = GenerationType.UUID)
    private UUID id;

    private LocalDateTime timestamp;
    private String level;
    private String message;
    private String userId;
    private String context;

    @Builder
    public ApplicationLogEntity(LocalDateTime timestamp, String level, String message, String exception, String userId, String context) {
        this.timestamp = timestamp;
        this.level = level;
        this.message = message;
        this.userId = userId;
        this.context = context;
    }
}
