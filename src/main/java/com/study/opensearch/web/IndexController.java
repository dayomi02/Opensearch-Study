package com.study.opensearch.web;

import com.study.opensearch.index.application.IndexService;
import lombok.RequiredArgsConstructor;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.Map;

@RestController
@RequestMapping("/api/index")
@RequiredArgsConstructor
public class IndexController {
    private final IndexService indexService;

    @PostMapping("/getMongoContents")
    public ResponseEntity<?> getMongoContents(@RequestBody Map<String, Object> requestBody) {
        return ResponseEntity.ok(indexService.getCommunityDocuments(requestBody));
    }

    @PostMapping("/batch")
    public ResponseEntity<?> executeBatchIndex(@RequestBody Map<String, Object> requestBody) {
        return ResponseEntity.ok(indexService.executeBatchIndex(requestBody));
    }

    @DeleteMapping("/delete/{indexName}")
    public ResponseEntity<?> deleteIndex(@PathVariable(name = "indexName") String indexName){
        return ResponseEntity.ok(indexService.deleteIndex(indexName));
    }
}
