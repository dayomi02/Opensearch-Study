package com.study.opensearch.batch.module;

import com.mongodb.client.*;
import com.study.opensearch.util.Constants;
import lombok.extern.slf4j.Slf4j;
import org.bson.Document;
import org.springframework.batch.item.ItemReader;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

@Slf4j
@Component
public class MongoItemReader implements ItemReader<Document> {

    private final MongoCursor<Document> cursor;

    @Value("${spring.data.mongodb.uri}")
    private String mongodbUri;

    @Value("${spring.data.mongodb.database}")
    private String mongodbDatabase;

    @Value("${index.docs.keyCode}")
    private String indexDocsKeyCode;

    private int count = 0;

    public MongoItemReader(){
        MongoClient mongoClient = MongoClients.create("mongodb://root:qlalfqjsgh@192.168.0.50:27017/?retryWrites=true&serverSelectionTimeoutMS=5000&connectTimeoutMS=10000&authSource=admin&authMechanism=SCRAM-SHA-256");
        MongoDatabase database = mongoClient.getDatabase("crawler_news");
        MongoCollection<Document> collection = database.getCollection("community_contents");
        this.cursor = collection.find(new Document(Constants.DOC_KEY_CODE, "BLIND")).iterator();
    }

    @Override
    public Document read(){
        if(cursor.hasNext()){
//            log.info(":::::::::::: read : {}", count++);
            return cursor.next();
        }else{
            return null;
        }
    }
}
