package dev.langchain4j.store.embedding.cloudsql;

import static com.fasterxml.jackson.databind.SerializationFeature.INDENT_OUTPUT;
import static dev.langchain4j.internal.Utils.getOrDefault;
import static dev.langchain4j.internal.Utils.isNotNullOrBlank;
import static dev.langchain4j.internal.Utils.isNotNullOrEmpty;
import static dev.langchain4j.internal.Utils.randomUUID;
import static dev.langchain4j.internal.ValidationUtils.*;
import static java.util.Collections.singletonList;
import static java.util.stream.Collectors.toList;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.pgvector.PGvector;
import dev.langchain4j.data.document.Metadata;
import dev.langchain4j.data.embedding.Embedding;
import dev.langchain4j.data.segment.TextSegment;
import dev.langchain4j.engine.PostgresEngine;
import dev.langchain4j.store.embedding.EmbeddingMatch;
import dev.langchain4j.store.embedding.EmbeddingSearchRequest;
import dev.langchain4j.store.embedding.EmbeddingSearchResult;
import dev.langchain4j.store.embedding.EmbeddingStore;
import dev.langchain4j.store.embedding.filter.CloudsqlFilterMapper;
import dev.langchain4j.store.embedding.index.DistanceStrategy;
import dev.langchain4j.store.embedding.index.query.QueryOptions;
import java.sql.Array;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.util.*;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PostgresEmbeddingStore implements EmbeddingStore<TextSegment> {

    private static final Logger log = LoggerFactory.getLogger(PostgresEmbeddingStore.class.getName());
    private final PostgresEngine engine;
    private final String tableName;
    private String schemaName;
    private String contentColumn;
    private String embeddingColumn;
    private String idColumn;
    private List<String> metadataColumns;
    private final Integer k;
    private final Integer fetchK;
    private final Double lambdaMult;

    private final CloudsqlFilterMapper FILTER_MAPPER = new CloudsqlFilterMapper();
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper().enable(INDENT_OUTPUT);

    private QueryOptions queryOptions;
    private String metadataJsonColumn;
    private final DistanceStrategy distanceStrategy;

    /**
     * Constructor for PostgresEmbeddingStore
     *
     * @param builder builder
     */
    public PostgresEmbeddingStore(Builder builder) {
        this.engine = builder.engine;
        this.tableName = builder.tableName;
        this.schemaName = builder.schemaName;
        this.contentColumn = builder.contentColumn;
        this.embeddingColumn = builder.embeddingColumn;
        this.idColumn = builder.idColumn;
        this.metadataColumns = builder.metadataColumns;
        this.queryOptions = builder.queryOptions;
        this.metadataJsonColumn = builder.metadataJsonColumn;
        this.distanceStrategy = builder.distanceStrategy;
        this.k = builder.k;
        this.fetchK = builder.fetchK;
        this.lambdaMult = builder.lambdaMult;
        // check columns exist in the table
        verifyEmbeddingStoreColumns(builder.ignoreMetadataColumnNames);
    }

    private void verifyEmbeddingStoreColumns(List<String> ignoredColumns) {
        if (!metadataColumns.isEmpty() && !ignoredColumns.isEmpty()) {
            throw new IllegalArgumentException(
                    "Cannot use both metadataColumns and ignoreMetadataColumns at the same time.");
        }

        String query = String.format(
                "SELECT column_name, data_type FROM information_schema.columns WHERE table_name ="
                        + " \"%s\" AND table_schema = \"%s\"",
                tableName, schemaName);

        Map<String, String> allColumns = new HashMap();

        try (Connection conn = engine.getConnection();
                ResultSet resultSet = conn.createStatement().executeQuery(query)) {
            ResultSetMetaData rsMeta = resultSet.getMetaData();
            int columnCount = rsMeta.getColumnCount();
            for (int i = 1; i <= columnCount; i++) {
                allColumns.put(rsMeta.getColumnName(i), rsMeta.getColumnTypeName(i));
            }

            if (!allColumns.containsKey(idColumn)) {
                throw new IllegalStateException("Id column, " + idColumn + ", does not exist.");
            }
            if (!allColumns.containsKey(contentColumn)) {
                throw new IllegalStateException("Content column, " + contentColumn + ", does not exist.");
            }
            if (!allColumns.get(contentColumn).equalsIgnoreCase("text")
                    || !allColumns.get(contentColumn).contains("char")) {
                throw new IllegalStateException("Content column, is type "
                        + allColumns.get(contentColumn)
                        + ". It must be a type of character string.");
            }
            if (!allColumns.containsKey(embeddingColumn)) {
                throw new IllegalStateException("Embedding column, " + embeddingColumn + ", does not exist.");
            }
            if (!allColumns.get(embeddingColumn).equalsIgnoreCase("USER-DEFINED")) {
                throw new IllegalStateException("Embedding column, " + embeddingColumn + ", is not type Vector.");
            }
            if (!allColumns.containsKey(metadataJsonColumn)) {
                metadataJsonColumn = null;
            }

            for (String metadataColumn : metadataColumns) {
                if (!allColumns.containsKey(metadataColumn)) {
                    throw new IllegalStateException("Metadata column, " + metadataColumn + ", does not exist.");
                }
            }

            if (ignoredColumns != null && !ignoredColumns.isEmpty()) {

                Map<String, String> allColumnsCopy =
                        allColumns.entrySet().stream().collect(Collectors.toMap(e -> e.getKey(), e -> e.getValue()));
                ignoredColumns.add(idColumn);
                ignoredColumns.add(contentColumn);
                ignoredColumns.add(embeddingColumn);

                for (String ignore : ignoredColumns) {
                    allColumnsCopy.remove(ignore);
                }

                metadataColumns.addAll(allColumnsCopy.keySet());
            }

        } catch (SQLException ex) {
            throw new RuntimeException(
                    "Exception caught when verifying vector store table: \"" + schemaName + "\".\"" + tableName + "\"",
                    ex);
        }
    }

    /**
     * Adds multiple embeddings to the store.
     *
     * @param embeddings a list of embeddings to be added to the store.
     * @param embedded a list of original contents that were embedded.
     */
    @Override
    public List<String> addAll(List<Embedding> embeddings, List<TextSegment> textSegment) {
        List<String> ids = embeddings.stream().map(ignored -> randomUUID()).collect(toList());
        addAll(ids, embeddings, textSegment);
        return ids;
    }

    /**
     * Adds multiple embeddings to the store.
     *
     * @param embeddings a list of embeddings to be added to the store.
     */
    @Override
    public List<String> addAll(List<Embedding> embeddings) {
        List<String> ids = embeddings.stream().map(ignored -> randomUUID()).collect(toList());
        List<TextSegment> nullTextSegments = Collections.nCopies(ids.size(), (TextSegment) null);
        addAll(ids, embeddings, nullTextSegments);
        return ids;
    }

    /**
     * Adds a given embedding to the store.
     *
     * @param embedding the embedding to be added to the store.
     */
    @Override
    public String add(Embedding embedding) {
        String id = randomUUID();
        addInternal(id, embedding, null);
        return id;
    }

    /**
     * Adds a given embedding to the store.
     *
     * @param embedding the embedding to be added to the store.
     * @param textSegment original content that was embedded.
     */
    @Override
    public String add(Embedding embedding, TextSegment textSegment) {
        String id = randomUUID();
        addInternal(id, embedding, textSegment);
        return id;
    }

    /**
     * Adds a given embedding to the store.
     *
     * @param embedding The embedding to be added to the store.
     */
    @Override
    public void add(String id, Embedding embedding) {
        addInternal(id, embedding, null);
    }

    private void addInternal(String id, Embedding embedding, TextSegment textSegment) {
        addAll(singletonList(id), singletonList(embedding), singletonList(textSegment));
    }

    @Override
    public void addAll(List<String> ids, List<Embedding> embeddings, List<TextSegment> textSegments) {
        if (ids.size() != embeddings.size() || embeddings.size() != textSegments.size()) {
            throw new IllegalArgumentException(
                    "List parameters ids and embeddings and textSegments shouldn't be different sizes!");
        }
        try (Connection connection = engine.getConnection()) {
            String metadataColumnNames =
                    metadataColumns.stream().map(column -> "\"" + column + "\"").collect(Collectors.joining(", "));

            // idColumn, contentColumn and embeddedColumn
            int totalColumns = 3;

            if (isNotNullOrEmpty(metadataColumnNames)) {
                totalColumns += metadataColumnNames.split(",").length;
                metadataColumnNames = ", " + metadataColumnNames;
            }

            if (isNotNullOrEmpty(metadataJsonColumn)) {
                metadataColumnNames += ", \"" + metadataJsonColumn + "\"";
                totalColumns++;
            }

            String placeholders = "?";
            for (int p = 1; p < totalColumns; p++) {
                placeholders += ", ?";
            }

            String query = String.format(
                    "INSERT INTO \"%s\".\"%s\" (\"%s\", \"%s\", \"%s\"%s) VALUES (%s)",
                    schemaName, tableName, idColumn, contentColumn, embeddingColumn, metadataColumnNames, placeholders);
            try (PreparedStatement preparedStatement = connection.prepareStatement(query)) {
                for (int i = 0; i < ids.size(); i++) {
                    String id = ids.get(i);
                    Embedding embedding = embeddings.get(i);
                    TextSegment textSegment = textSegments.get(i);
                    String text = textSegment != null ? textSegment.text() : null;
                    // assume metadata is always present
                    // langchain4j/langchain4j-core/src/main/java/dev/langchain4j/data/segment/TextSegment.java L30
                    Map<String, Object> embeddedMetadataCopy = textSegment.metadata().toMap().entrySet().stream()
                            .collect(Collectors.toMap(e -> e.getKey(), e -> e.getValue()));
                    preparedStatement.setString(1, id);
                    preparedStatement.setObject(2, embedding);
                    preparedStatement.setString(3, text);
                    int j = 4;
                    if (embeddedMetadataCopy != null && !embeddedMetadataCopy.isEmpty()) {
                        for (; j < metadataColumns.size(); j++) {
                            if (embeddedMetadataCopy.containsKey(metadataColumns.get(j))) {
                                preparedStatement.setObject(j, embeddedMetadataCopy.remove(metadataColumns.get(j)));
                            } else {
                                preparedStatement.setObject(j, null);
                            }
                        }
                        if (isNotNullOrEmpty(metadataJsonColumn)) {
                            // metadataJsonColumn should be the last column left
                            preparedStatement.setObject(
                                    j, OBJECT_MAPPER.writeValueAsString(embeddedMetadataCopy), Types.OTHER);
                        }
                    } else {
                        for (; j < metadataColumns.size(); j++) {
                            preparedStatement.setObject(j, null);
                        }
                    }
                    preparedStatement.addBatch();
                }
                preparedStatement.executeBatch();
            } catch (JsonProcessingException ex) {
                throw new RuntimeException("Exception caught when processing JSON metadata", ex);
            }

        } catch (SQLException ex) {
            throw new RuntimeException(
                    "Exception caught when inserting into vector store table: \""
                            + schemaName
                            + "\".\""
                            + tableName
                            + "\"",
                    ex);
        }
    }

    @Override
    public void removeAll(Collection<String> ids) {
        if (ids == null || ids.isEmpty()) {
            throw new IllegalArgumentException("ids must not be null or empty");
        }

        String query = String.format("DELETE FROM \"%s\".\"%s\" WHERE %s IN (?)", schemaName, tableName, idColumn);

        try (Connection conn = engine.getConnection()) {
            try (PreparedStatement preparedStatement = conn.prepareStatement(query)) {
                Array array = conn.createArrayOf(
                        "uuid", ids.stream().map(UUID::fromString).toArray());
                preparedStatement.setArray(1, array);
                preparedStatement.executeUpdate();
            }
        } catch (SQLException ex) {
            log.error(
                    String.format(
                            "Exception caught when inserting into vector store table: \"%s\".\"%s\"",
                            schemaName, tableName),
                    ex);
            throw new RuntimeException(ex);
        }
    }

    @Override
    public EmbeddingSearchResult<TextSegment> search(EmbeddingSearchRequest request) {
        List<String> columns = new ArrayList<String>(metadataColumns);
        columns.add(idColumn);
        columns.add(contentColumn);
        columns.add(embeddingColumn);
        if (isNotNullOrBlank(metadataJsonColumn)) {
            columns.add(metadataJsonColumn);
        }

        String columnNames =
                columns.stream().map(c -> String.format("\"%s\"", c)).collect(Collectors.joining(", "));

        String filterString = FILTER_MAPPER.map(request.filter());

        String whereClause = isNotNullOrBlank(filterString) ? String.format("WHERE %s", filterString) : "";

        String vector =
                String.format("'%s'", Arrays.toString(request.queryEmbedding().vector()));

        String query = String.format(
                "SELECT %s, %s(%s, %s) as distance FROM \"%s\".\"%s\" %s ORDER BY %s %s %s LIMIT %d;",
                columnNames,
                distanceStrategy.getSearchFunction(),
                embeddingColumn,
                vector,
                schemaName,
                tableName,
                whereClause,
                embeddingColumn,
                distanceStrategy.getOperator(),
                vector,
                request.maxResults());

        List<EmbeddingMatch<TextSegment>> embeddingMatches = new ArrayList<>();

        try (Connection conn = engine.getConnection()) {
            try (Statement statement = conn.createStatement()) {
                if (queryOptions != null) {
                    for (String option : queryOptions.getParameterSettings()) {
                        statement.executeQuery(String.format("SET LOCAL %s;", option));
                    }
                }
                ResultSet resultSet = statement.executeQuery(query);
                while (resultSet.next()) {
                    double distance = resultSet.getDouble("distance");
                    String embeddingId = resultSet.getString(idColumn);

                    PGvector pgVector = (PGvector) resultSet.getObject(embeddingColumn);

                    Embedding embedding = Embedding.from(pgVector.toArray());

                    String embeddedText = resultSet.getString(contentColumn);
                    Map<String, Object> metadataMap = new HashMap<>();

                    for (String metaColumn : metadataColumns) {
                        metadataMap.put(metaColumn, resultSet.getObject(metaColumn));
                    }

                    if (isNotNullOrBlank(metadataJsonColumn)) {
                        String metadataJsonString = getOrDefault(resultSet.getString(metadataJsonColumn), "{}");
                        Map<String, Object> metadataJsonMap = OBJECT_MAPPER.readValue(metadataJsonString, Map.class);
                        metadataMap.putAll(metadataJsonMap);
                    }

                    Metadata metadata = Metadata.from(metadataMap);

                    TextSegment embedded = new TextSegment(embeddedText, metadata);

                    embeddingMatches.add(new EmbeddingMatch<>(distance, embeddingId, embedding, embedded));
                }
            } catch (JsonProcessingException ex) {
                throw new RuntimeException("Exception caught when processing JSON metadata", ex);
            }
        } catch (SQLException ex) {
            throw new RuntimeException(
                    "Exception caught when searching in store table: \"" + schemaName + "\".\"" + tableName + "\"", ex);
        }
        return new EmbeddingSearchResult<>(embeddingMatches);
    }

    /** Builder which configures and creates instances of {@link PostgresEmbeddingStore}. */
    public static class Builder {

        private PostgresEngine engine;
        private final String tableName;
        private String schemaName = "public";
        private String contentColumn = "content";
        private String embeddingColumn = "embedding";
        private String idColumn = "langchain_id";
        private List<String> metadataColumns = new ArrayList<>();
        private String metadataJsonColumn = "langchain_metadata";
        private List<String> ignoreMetadataColumnNames = new ArrayList<>();
        private DistanceStrategy distanceStrategy = DistanceStrategy.COSINE_DISTANCE;
        private Integer k = 4;
        private Integer fetchK = 20;
        private Double lambdaMult = 0.5;
        private QueryOptions queryOptions;

        /**
         * Constructor for Builder
         *
         * @param engine required {@link PostgresEngine}
         * @param tableName table to be used as embedding store
         */
        public Builder(PostgresEngine engine, String tableName) {
            this.engine = engine;
            this.tableName = tableName;
        }

        /**
         * @param schemaName (Default: "public") The schema name * @return this builder
         */
        public Builder schemaName(String schemaName) {
            this.schemaName = schemaName;
            return this;
        }

        /**
         * @param contentColumn (Default: "content") create the content column with custom name
         *     * @return this builder
         */
        public Builder contentColumn(String contentColumn) {
            this.contentColumn = contentColumn;
            return this;
        }

        /**
         * @param embeddingColumn (Default: "embedding") create the embedding column with custom name
         *     * @return this builder
         */
        public Builder embeddingColumn(String embeddingColumn) {
            this.embeddingColumn = embeddingColumn;
            return this;
        }

        /**
         * @param idColumn (Optional, Default: "langchain_id") Column to store ids. * @return this
         *     builder
         */
        public Builder idColumn(String idColumn) {
            this.idColumn = idColumn;
            return this;
        }

        /**
         * @param metadataColumns list of SQLAlchemy Columns to create for custom metadata * @return
         *     this builder
         */
        public Builder metadataColumns(List<String> metadataColumns) {
            this.metadataColumns = metadataColumns;
            return this;
        }

        /**
         * @param metadataJsonColumn (Default: "langchain_metadata") the column to store extra metadata
         *     in * @return this builder
         */
        public Builder metadataJsonColumn(String metadataJsonColumn) {
            this.metadataJsonColumn = metadataJsonColumn;
            return this;
        }

        /**
         * @param ignoreMetadataColumnNames (Optional) Column(s) to ignore in pre-existing tables for a
         *     document’s * @return this builder
         */
        public Builder ignoreMetadataColumnNames(List<String> ignoreMetadataColumnNames) {
            this.ignoreMetadataColumnNames = ignoreMetadataColumnNames;
            return this;
        }

        /**
         * @param distanceStrategy (Defaults: COSINE_DISTANCE) Distance strategy to use for vector
         *     similarity search * @return this builder
         */
        public Builder distanceStrategy(DistanceStrategy distanceStrategy) {
            this.distanceStrategy = distanceStrategy;
            return this;
        }

        /**
         * @param k (Defaults: 4) Number of Documents to return from search * @return this builder
         */
        public Builder k(Integer k) {
            this.k = k;
            return this;
        }

        /**
         * @param fetchK (Defaults: 20) Number of Documents to fetch to pass to MMR algorithm * @return
         *     this builder
         */
        public Builder fetchK(Integer fetchK) {
            this.fetchK = fetchK;
            return this;
        }

        /**
         * @param queryOptions (Optional) QueryOptions class with vector search parameters * @return
         *     this builder
         */
        public Builder queryOptions(QueryOptions queryOptions) {
            this.queryOptions = queryOptions;
            return this;
        }

        /**
         * @param lambdaMult (Defaults: 0.5): Number between 0 and 1 that determines the degree of
         *     diversity among the results with 0 corresponding to maximum diversity and 1 to minimum
         *     diversity * @return this builder
         */
        public Builder lambdaMult(Double lambdaMult) {
            this.lambdaMult = lambdaMult;
            return this;
        }

        /**
         * Builds an {@link PostgresEmbeddingStore} store with the configuration applied to this
         * builder.
         *
         * @return A new {@link PostgresEmbeddingStore} instance
         */
        public PostgresEmbeddingStore build() {
            return new PostgresEmbeddingStore(this);
        }
    }
}
