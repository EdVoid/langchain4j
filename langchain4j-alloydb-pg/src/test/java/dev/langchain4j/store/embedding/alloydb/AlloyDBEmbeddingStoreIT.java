package dev.langchain4j.store.embedding.alloydb;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import dev.langchain4j.data.embedding.Embedding;
import dev.langchain4j.engine.AlloyDBEngine;
import dev.langchain4j.engine.EmbeddingStoreConfig;
import static dev.langchain4j.utils.AlloyDBTestUtils.randomVector;

public class AlloyDBEmbeddingStoreIT {
    private static final String TABLE_NAME = "JAVA_EMBEDDING_TEST_TABLE";
    private static final Integer VECTOR_SIZE = 768;
    private static EmbeddingStoreConfig defaultParameters;
    private static String iamEmail;
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
        iamEmail = System.getenv("ALLOYDB_IAM_EMAIL");

        engine = new AlloyDBEngine.Builder().projectId(projectId).region(region).cluster(cluster).instance(instance)
                .database(database).user(user).password(password).ipType("PUBLIC").build();
        engine.initVectorStoreTable(defaultParameters);

        store = new AlloyDBEmbeddingStore.Builder(engine, TABLE_NAME).build();
        defaultConnection = engine.getConnection();

        defaultParameters = EmbeddingStoreConfig.builder().tableName(TABLE_NAME).vectorSize(VECTOR_SIZE).build();

    }

    @AfterEach
    public void afterEach() throws SQLException {
        defaultConnection.createStatement().executeUpdate(String.format("TRUNCATE TABLE IF EXISTS \"%s\"", TABLE_NAME));
    }

    @AfterAll
    public static void afterAll() throws SQLException {
        defaultConnection.createStatement().executeUpdate(String.format("DROP TABLE IF EXISTS \"%s\"", TABLE_NAME));
        defaultConnection.close();
    }

    @Test
    void add_single_embedding_to_store() {
        float[] vector = randomVector(5);
        Embedding embedding = new Embedding(vector);
        String id = store.add(embedding);

        try(Statement statement = defaultConnection.createStatement();) {
            ResultSet rs = statement.executeQuery(String.format("SELECT \"%s\" FROM \"%s\" WHERE \"%s\" = %s", defaultParameters.getEmbeddingColumn(),TABLE_NAME, defaultParameters.getIdColumn(), id));
            rs.next();
            String response = rs.getString(defaultParameters.getEmbeddingColumn());
            assertThat(response).isEqualTo(Arrays.toString(vector));
        } catch (SQLException ex) {
        }
    }

    @Test
    void add_embeddings_list_to_store() {
        List<String> expectedVectors = new ArrayList<>();
        List<Embedding> embeddings = new ArrayList<>();
        for(int i = 0; i < 10; i++){
            float[] vector = randomVector(5);
            expectedVectors.add(Arrays.toString(vector));
            embeddings.add(new Embedding(vector));
        }
        String ids = String.join(",", store.addAll(embeddings));

        try(Statement statement = defaultConnection.createStatement();) {
            ResultSet rs = statement.executeQuery(String.format("SELECT \"%s\" FROM \"%s\" WHERE \"%s\" IN (%s)", defaultParameters.getEmbeddingColumn(),TABLE_NAME, defaultParameters.getIdColumn(), ids));
            while(rs.next()) {
                String response = rs.getString(defaultParameters.getEmbeddingColumn());
                assertThat(expectedVectors).contains(response);
            }
        } catch (SQLException ex) {
        }
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
