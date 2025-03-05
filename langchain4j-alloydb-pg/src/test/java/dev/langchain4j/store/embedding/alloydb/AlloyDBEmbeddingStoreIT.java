package dev.langchain4j.store.embedding.alloydb;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import static com.fasterxml.jackson.databind.SerializationFeature.INDENT_OUTPUT;

import dev.langchain4j.data.document.Metadata;
import dev.langchain4j.data.embedding.Embedding;
import dev.langchain4j.data.segment.TextSegment;
import dev.langchain4j.engine.AlloyDBEngine;
import dev.langchain4j.engine.EmbeddingStoreConfig;
import dev.langchain4j.engine.MetadataColumn;
import static dev.langchain4j.internal.Utils.getOrDefault;
import static dev.langchain4j.internal.Utils.randomUUID;
import static dev.langchain4j.utils.AlloyDBTestUtils.randomVector;

public class AlloyDBEmbeddingStoreIT {
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper()
    .enable(INDENT_OUTPUT);
    private static final String TABLE_NAME = "JAVA_EMBEDDING_TEST_TABLE";
    private static final Integer VECTOR_SIZE = 5;
    private static EmbeddingStoreConfig embeddingStoreConfig;
    private static String projectId;
    private static String region;
    private static String cluster;
    private static String instance;
    private static String database;
    private static String user;
    private static String password;

    private static AlloyDBEngine engine;
    private static AlloyDBEmbeddingStore store;
    private static Connection defaultConnection;

    @BeforeAll
    public static void beforeAll() throws SQLException {
        projectId = System.getenv("ALLOYDB_PROJECT_ID");
        region = System.getenv("ALLOYDB_REGION");
        cluster = System.getenv("ALLOYDB_CLUSTER");
        instance = System.getenv("ALLOYDB_INSTANCE");
        database = System.getenv("ALLOYDB_DB_NAME");
        user = System.getenv("ALLOYDB_USER");
        password = System.getenv("ALLOYDB_PASSWORD");

        engine = new AlloyDBEngine.Builder().projectId(projectId).region(region).cluster(cluster).instance(instance)
                .database(database).user(user).password(password).ipType("PUBLIC").build();

        List<MetadataColumn> metadataColumns = new ArrayList<>();
        metadataColumns.add(new MetadataColumn("string", "text", true));
        metadataColumns.add(new MetadataColumn("uuid", "uuid", true));
        metadataColumns.add(new MetadataColumn("integer", "integer", true));
        metadataColumns.add(new MetadataColumn("long", "bigint", true));
        metadataColumns.add(new MetadataColumn("float", "real", true));
        metadataColumns.add(new MetadataColumn("double", "double precision", true));

        embeddingStoreConfig = EmbeddingStoreConfig.builder().tableName(TABLE_NAME)
        .vectorSize(VECTOR_SIZE).metadataColumns(metadataColumns).storeMetadata(true).build();

        defaultConnection = engine.getConnection();

        defaultConnection.createStatement().executeUpdate(String.format("DROP TABLE IF EXISTS \"%s\"", TABLE_NAME));

        engine.initVectorStoreTable(embeddingStoreConfig);

        List<String> metaColumnNames = metadataColumns.stream().map(c -> c.getName()).collect(Collectors.toList());

        store = new AlloyDBEmbeddingStore.Builder(engine, TABLE_NAME).metadataColumns(metaColumnNames).build();

    }

    @AfterEach
    public void afterEach() throws SQLException {
        defaultConnection.createStatement().executeUpdate(String.format("TRUNCATE TABLE \"%s\"", TABLE_NAME));
    }

    @AfterAll
    public static void afterAll() throws SQLException {
        defaultConnection.createStatement().executeUpdate(String.format("DROP TABLE IF EXISTS \"%s\"", TABLE_NAME));
        defaultConnection.close();
    }

    @Test
    void add_single_embedding_to_store() throws SQLException {
        float[] vector = randomVector(5);
        Embedding embedding = new Embedding(vector);
        String id = store.add(embedding);

        try(Statement statement = defaultConnection.createStatement();) {
            ResultSet rs = statement.executeQuery(String.format("SELECT \"%s\" FROM \"%s\" WHERE \"%s\" = '%s'",
             embeddingStoreConfig.getEmbeddingColumn(),TABLE_NAME, embeddingStoreConfig.getIdColumn(), id));
            rs.next();
            String response = rs.getString(embeddingStoreConfig.getEmbeddingColumn());
            assertThat(response).isEqualTo(Arrays.toString(vector).replaceAll("\s", ""));
        }
    }

    @Test
    void add_embeddings_list_to_store() throws SQLException {
        List<String> expectedVectors = new ArrayList<>();
        List<Embedding> embeddings = new ArrayList<>();
        for(int i = 0; i < 10; i++){
            float[] vector = randomVector(5);
            expectedVectors.add(Arrays.toString(vector).replaceAll("\s", ""));
            embeddings.add(new Embedding(vector));
        }
        List<String> ids = store.addAll(embeddings);
        String stringIds = ids.stream().map(id -> String.format("'%s'", id)).collect(Collectors.joining(","));

        try(Statement statement = defaultConnection.createStatement();) {
            ResultSet rs = statement.executeQuery(String.format("SELECT \"%s\" FROM \"%s\" WHERE \"%s\" IN (%s)",
             embeddingStoreConfig.getEmbeddingColumn(),TABLE_NAME, embeddingStoreConfig.getIdColumn(), stringIds));
            while(rs.next()) {
                String response = rs.getString(embeddingStoreConfig.getEmbeddingColumn());
                assertThat(expectedVectors).contains(response);
            }
        }
    }

    @Test
    void add_single_embedding_with_id_to_store() throws SQLException {
        float[] vector = randomVector(5);
        Embedding embedding = new Embedding(vector);
        String id = randomUUID();
        store.add(id, embedding);

        try(Statement statement = defaultConnection.createStatement();) {
            ResultSet rs = statement.executeQuery(String.format("SELECT \"%s\" FROM \"%s\" WHERE \"%s\" = '%s'",
             embeddingStoreConfig.getEmbeddingColumn(),TABLE_NAME, embeddingStoreConfig.getIdColumn(), id));
            rs.next();
            String response = rs.getString(embeddingStoreConfig.getEmbeddingColumn());
            assertThat(response).isEqualTo(Arrays.toString(vector).replaceAll("\s", ""));
        }
    }

    @Test
    void add_single_embedding_with_content_to_store() throws SQLException, JsonProcessingException {
        float[] vector = randomVector(5);
        Embedding embedding = new Embedding(vector);
        
        Map<String, Object> metaMap = new HashMap();
        metaMap.put("string", "s");
        metaMap.put("uuid", UUID.randomUUID());
        metaMap.put("integer", 1);
        metaMap.put("long", 1L);
        metaMap.put("float", 1f);
        metaMap.put("double", 1d);
        metaMap.put("extra", "not in table columns");
        metaMap.put("extra_credits", 10);
        Metadata metadata = new Metadata(metaMap);
        TextSegment textSegment =  new TextSegment("this is a test text", metadata);
        String id = store.add(embedding, textSegment);

        String metadataColumnNames = metaMap.entrySet().stream()
                    .filter(e -> !e.getKey().contains("extra")).map(e -> "\"" + e.getKey() + "\"").collect(Collectors.joining(", "));

        try(Statement statement = defaultConnection.createStatement();) {
            ResultSet rs = statement.executeQuery(String.format("SELECT \"%s\", %s, \"%s\" FROM \"%s\" WHERE \"%s\" = '%s'",
             embeddingStoreConfig.getEmbeddingColumn(), metadataColumnNames, embeddingStoreConfig.getMetadataJsonColumn(), TABLE_NAME, embeddingStoreConfig.getIdColumn(), id));
            Map<String, Object> extraMetaMap = new HashMap<>();
            Map<String, Object> metadataJsonMap = null;
            while(rs.next()) {
                String response = rs.getString(embeddingStoreConfig.getEmbeddingColumn());
                assertThat(response).isEqualTo(Arrays.toString(vector).replaceAll("\s", ""));
                for(String column : metaMap.keySet()) {
                    if(column.contains("extra")) {
                        extraMetaMap.put(column, metaMap.get(column));
                    } else {
                        assertThat(rs.getObject(column)).isEqualTo(metaMap.get(column));
                    }
                }
                String metadataJsonString = getOrDefault(rs.getString(embeddingStoreConfig.getMetadataJsonColumn()), "{}");
                metadataJsonMap = OBJECT_MAPPER.readValue(metadataJsonString, Map.class);    
            }
            assertThat(extraMetaMap.size()).isEqualTo(metadataJsonMap.size());
            for(String key : extraMetaMap.keySet()) {
                assertThat(extraMetaMap.get(key).equals((metadataJsonMap.get(key)))).isTrue();
            }
        } 
    }

    @Test
    void add_embeddings_list_and_content_list_to_store() throws SQLException, JsonProcessingException {
        Map<String, Integer> expectedVectorsAndIndexes = new HashMap<>();
        Map<Integer, Map<String, Object>> metaMaps = new HashMap<>();
        List<Embedding> embeddings = new ArrayList<>();
        List<TextSegment> textSegments = new ArrayList<>();

        for(int i = 0; i < 10; i++){
            float[] vector = randomVector(5);
            expectedVectorsAndIndexes.put(Arrays.toString(vector).replaceAll("\s",""), i);
            embeddings.add(new Embedding(vector));
            Map<String, Object> metaMap = new HashMap();
            metaMap.put("string", "s" + i);
            metaMap.put("uuid", UUID.randomUUID());
            metaMap.put("integer", i);
            metaMap.put("long", 1L);
            metaMap.put("float", 1f);
            metaMap.put("double", 1d);
            metaMap.put("extra", "not in table columns " + i);
            metaMap.put("extra_credits", 100 + i);
            metaMaps.put(i, metaMap);
            Metadata metadata = new Metadata(metaMap);
            textSegments.add(new TextSegment("this is a test text " + i, metadata));
        }

        List<String> ids = store.addAll(embeddings, textSegments);
        String stringIds = ids.stream().map(id -> String.format("'%s'", id)).collect(Collectors.joining(","));

        String metadataColumnNames = metaMaps.get(0).entrySet().stream()
                    .filter(e -> !e.getKey().contains("extra")).map(e -> "\"" + e.getKey() + "\"").collect(Collectors.joining(", "));

        try(Statement statement = defaultConnection.createStatement();) {
            ResultSet rs = statement.executeQuery(String.format("SELECT \"%s\", %s ,\"%s\" FROM \"%s\" WHERE \"%s\" IN (%s)",
             embeddingStoreConfig.getEmbeddingColumn(), metadataColumnNames, embeddingStoreConfig.getMetadataJsonColumn(), TABLE_NAME, embeddingStoreConfig.getIdColumn(), stringIds));
            Map<String, Object> extraMetaMap = new HashMap<>();
            Map<String, Object> metadataJsonMap = null;
            while(rs.next()) {
                String response = rs.getString(embeddingStoreConfig.getEmbeddingColumn());
                assertThat(expectedVectorsAndIndexes.keySet()).contains(response);
                int index = expectedVectorsAndIndexes.get(response);
                for(String column : metaMaps.get(index).keySet()) {
                    if(column.contains("extra")) {
                        extraMetaMap.put(column, metaMaps.get(index).get(column));
                    } else {
                        assertThat(rs.getObject(column)).isEqualTo(metaMaps.get(index).get(column));
                    }
                }
                String metadataJsonString = getOrDefault(rs.getString(embeddingStoreConfig.getMetadataJsonColumn()), "{}");
                metadataJsonMap = OBJECT_MAPPER.readValue(metadataJsonString, Map.class);    
            }
            assertThat(metadataJsonMap).isNotNull();
            assertThat(extraMetaMap.size()).isEqualTo(metadataJsonMap.size());
            for(String key : extraMetaMap.keySet()) {
                assertThat(extraMetaMap.get(key).equals((metadataJsonMap.get(key)))).isTrue();
            }
        } 
    }
}
