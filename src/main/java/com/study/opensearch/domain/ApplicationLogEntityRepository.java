package com.study.opensearch.domain;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;
import org.springframework.transaction.annotation.Transactional;

import java.time.LocalDateTime;

@Repository
public interface ApplicationLogEntityRepository extends JpaRepository<ApplicationLogEntity, Integer> {

    @Modifying
    @Query("DELETE FROM ApplicationLogEntity a WHERE a.timestamp < :timestamp")
    @Transactional
    void deleteLogBefore(@Param("timestamp") LocalDateTime timestamp);
}
