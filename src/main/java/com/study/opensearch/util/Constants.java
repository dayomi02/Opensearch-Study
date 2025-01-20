package com.study.opensearch.util;

public class Constants {
    // Document 관련 - db column
    public static final String DOC_COLLECTION_NAME = "community_contents_20000";

    public static final String DOC_ID = "_id";

    public static final String DOC_KEY_CODE = "key_code";

    public static final String DOC_IS_INDEXED = "is_indexed";

    // Request, Response 관련
    public static final String REQ_INDEX_NAME = "indexName";

    public static final String RES_INDEX_NAME = "indexName";

    public static final String REQ_KEY_CODE = "keyCode";

    public static final String RES_DOCUMENT_COUNT = "documentCount";

    public static final String RES_MESSAGE = "message";

    // Index 관련
    public static final String INDEX__INDEX = "_index";

    public static final String INDEX_INDEX = "index";

    public static final String INDEX__ID = "_id";

    public static final String INDEX_URL = "url";

    public static final String INDEX_CATEGORY = "category";

    public static final String INDEX_CONTENT = "content";

    public static final String INDEX_DESCRIPTION = "description";

    public static final String INDEX_KEY_CODE = "keyCode";

    public static final String INDEX_NAME = "name";

    public static final String INDEX_PUBLISH_DATE = "publishDate";

    public static final String INDEX_TITLE = "title";

    public static final String INDEX_QUERY = "query";

    // Spring Batch Job parameter
    public static final String JOB_PARAMETER_CURRENT_DATE = "currentDate";

    public static final String JOB_PARAMETER_JSON_FILE_PATH = "jsonFilePath";

    public static final String JOB_PARAMETER_INDEX_NAME = "indexName";

    // Logging
    public static final String LOG_LEVEL_INFO = "INFO";

    public static final String LOG_USER_ADMIN = "ADMIN";

    public static final String LOG_METHOD_START = "::: [{}] method Start :::";

    // Etc
    public static final String HTTP = "http";

    public static final String FLAG_Y = "Y";

    public static final String FLAG_N = "N";


    // Date format
    public static final String DATE_FORMAT = "yyyyMMddHHmm";
}
