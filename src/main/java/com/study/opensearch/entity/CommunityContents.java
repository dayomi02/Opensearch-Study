package com.study.opensearch.entity;

import com.study.opensearch.util.Constants;
import lombok.Getter;
import lombok.Setter;
import org.springframework.data.annotation.Id;
import org.springframework.data.mongodb.core.mapping.Document;
import org.springframework.data.mongodb.core.mapping.Field;

import java.time.LocalDateTime;

@Document(collection = Constants.DOC_COLLECTION_NAME)
//@Document//(collection = "${index.docs.collection}")
@Getter
@Setter
public class CommunityContents {

    @Id
    private String _id;

    /**
     * 카테고리.
     */
    private String category;

    /**
     * 카테고리1.
     */
//    @Field(name = StorageConfig.MONGO_FIELD_CATEGORY1)
    private String category1;

    /**
     * 카테고리2.
     */
//    @Field(name = StorageConfig.MONGO_FIELD_CATEGORY2)
    private String category2;

    /**
     * 카테고리3.
     */
//    @Field(name = StorageConfig.MONGO_FIELD_CATEGORY3)
    private String category3;

    /**
     * 내용.
     */
//    @Indexed(background = true, name = "content_asc", direction = IndexDirection.ASCENDING)
    @Field(name = "content")
    private String content;

    /**
     * 내용 HTML.
     */
    @Field(name = "content_html")
    private String contentHtml;

    /**
     * 수집 시간.
     */
//    @Indexed(background = true, name = "created_datetime_desc", direction = IndexDirection.DESCENDING)
    @Field(name = "created_datetime")
    private String createdDatetime;

    /**
     * 메타 og:description
     */
    private String description;

    /**
     * 확장 칼럼(Json 형태로 입력 바람).
     */
    private String extension;

    /**
     * 색인 여부
     */
    @Field(name = "is_indexed")
    private String isIndexed;

    /**
     * 수집 출처 고유키
     */
    @Field(name = "key_code")
    private String keyCode;

    /**
     * 명칭.
     */
//    @Indexed(background = true, name = "name_asc", direction = IndexDirection.ASCENDING)
    @Field(name = "name")
    private String name;

    /**
     * 원문 URL.
     */
    @Field(name = "origin_url")
    private String originUrl;

    /**
     * 발행일.
     */
//    @Indexed(background = true, name = "publish_date_desc", direction = IndexDirection.DESCENDING)
    @Field(name = "publish_date")
    private String publishDate;

    /**
     * 발행 시간.
     */
//    @Indexed(background = true, name = "publish_datetime_desc", direction = IndexDirection.DESCENDING)
    @Field(name = "publish_datetime")
    private LocalDateTime publishDatetime;

    /**
     * 발행월.
     */
    @Field(name = "publish_month")
    private String publishMonth;

    /**
     * 발행년.
     */
    @Field(name = "publish_year")
    private String publishYear;

    /**
     * 수집 키워드.
     */
    @Field(name = "query")
    private String query;

    /**
     * 작업명.
     */
    @Field(name = "task_name")
    private String taskName;

    /**
     * 제목.
     */
//    @Indexed(background = true, name = "title_asc", direction = IndexDirection.ASCENDING)
    @Field(name = "title")
    private String title;
}
