package dev.langchain4j.store.embedding.alloydb;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import static dev.langchain4j.store.embedding.alloydb.utils.VerifyUtils.verifyColumns;
import static dev.langchain4j.store.embedding.alloydb.utils.VerifyUtils.verifyIndex;

public class AlloyDBEmbeddingStoreIT {
    private static final String TABLE_NAME = "JAVA_EMBEDDING_TEST_TABLE";
    private static final Integer VECTOR_SIZE = 768;
    private static String projectId;
    private static String region;
    private static String cluster;
    private static String instance;
    private static String database;
    private static String user;
    private static String password;

    private static AlloyDBEngine engine;
    private static AlloyDBEmbeddingStore store;

    @BeforeAll
    public static void beforeAll() throws SQLException {
        projectId = System.getenv("ALLOYDB_PROJECT_ID");
        region = System.getenv("ALLOYDB_REGION");
        cluster = System.getenv("ALLOYDB_CLUSTER");
        instance = System.getenv("ALLOYDB_INSTANCE");
        database = System.getenv("ALLOYDB_DB_NAME");
        user = System.getenv("ALLOYDB_USER");
        password = System.getenv("ALLOYDB_PASSWORD");

        engine = AlloyDBEngine.builder().projectId(projectId).region(region).cluster(cluster).instance(instance).database(database).user(user).password(password).ipType("PUBLIC").build();
        // available after merging engine stuff
        // engine.initVectorStoreTable(TABLE_NAME, VECTOR_SIZE, null, null, null, null, null, null, false);
        engine.initVectorStoreTable();
        store = AlloyDBEmbeddingStore.builder().engine(engine).tableName(TABLE_NAME).build();

    }

    @AfterEach
    public void afterEach() throws SQLException {
        // clear data
    }

    @AfterAll
    public static void afterAll() throws SQLException {
        // drop table
    }

    @Test
    void initialize_default_embedding_store() throws SQLException {
        // stuff is private, maybe query column names
        Set<String> expectedNames = new HashSet<>();

        expectedNames.add("langchain_id");
        expectedNames.add("content");
        expectedNames.add("embedding");

        try(Connection connection = engine.getConnection()) {
            verifyColumns(connection, TABLE_NAME, expectedNames);
            verifyIndex(connection, TABLE_NAME, "hnsw", "USING hnsw (custom_embedding_column)");
        }
    }

    @Test
    void initialize_custom_embedding_store() throws SQLException {
        List<MetadataColumn> metadataColumns = new ArrayList<>();
        metadataColumns.add(new MetadataColumn("page", "TEXT", true));
        metadataColumns.add(new MetadataColumn("source", "TEXT", false));
        AlloyDBEmbeddingStore.builder().engine(engine).tableName(TABLE_NAME).contentColumn("custom_content_column").embeddingColumn("custom_embedding_column").embeddingIdColumn("custom_embedding_id_column").metadataColumns(metadataColumns).build();

        Set<String> expectedColumns = new HashSet<>();
        expectedColumns.add("embedding_id");
        expectedColumns.add("custom_embedding_id_column");
        expectedColumns.add("custom_content_column");
        expectedColumns.add("custom_embedding_column");
        expectedColumns.add("page");
        expectedColumns.add("source");
        try(Connection connection = engine.getConnection()) {
            verifyColumns(connection, TABLE_NAME, expectedColumns);
            verifyIndex(connection, TABLE_NAME, "ivfflat", "USING ivfflat (custom_embedding_column)");    
        }
    }

    @Test
    void add_single_embedding_to_store() {
        // TODO 
    }

    @Test
    void add_embeddings_list_to_store() {
        // TODO
    }

    @Test
    void add_single_embedding_with_id_to_store() {
        // TODO 
    }

    @Test
    void add_single_embedding_with_content_to_store() {
        // TODO 
    }

    @Test
    void add_embeddings_list_and_content_list_to_store() {
        // TODO
    }
}
