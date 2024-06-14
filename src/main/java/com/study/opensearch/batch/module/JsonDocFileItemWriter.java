package com.study.opensearch.batch.module;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import lombok.extern.slf4j.Slf4j;
import org.bson.Document;
import org.springframework.batch.item.Chunk;
import org.springframework.batch.item.ItemWriter;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.core.io.FileSystemResource;
import org.springframework.stereotype.Component;

import java.io.File;
import java.io.FileWriter;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;

@Slf4j
@Component
public class JsonDocFileItemWriter implements ItemWriter<Document> {
    private final File file;
    private final ObjectMapper objectMapper;

    @Value("${index.docs.path.source}")
    private String indexDocsPath;

    @Value("${index.docs.name}")
    private String indexDocsName;

    private int count = 0;

    public JsonDocFileItemWriter() {
        String currentDate = LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyyMMddHHmm"));
//        String filePath = indexDocsPath + indexDocsName + "_" + currentDate + ".json";
        String filePath = "src\\main\\resources\\files\\community_blind_2" + "_" + currentDate + ".json";

        this.file = new FileSystemResource(filePath).getFile();
        this.objectMapper = new ObjectMapper();

        objectMapper.enable(SerializationFeature.INDENT_OUTPUT);
    }

    @Override
    public void write(Chunk<? extends Document> chunk) throws Exception {
        List<Document> items = new ArrayList<>();

        try (FileWriter fileWriter = new FileWriter(file, true)){
            for (Document item : chunk) {
//                log.info("::: write : {}", count++);
//                count++;
                items.add(item);
            }
//            log.info("::: writeValue : {} ", count);
            objectMapper.writeValue(fileWriter, items);
        }
    }
}
