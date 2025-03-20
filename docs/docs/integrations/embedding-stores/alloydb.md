# AlloyDB PostgreSQL Embedding Store for LangChain4j


This module implements `EmbeddingStore` backed by an AlloyDB for PostgreSQL database.

### Maven Dependency

```xml
<dependency>
    <groupId>dev.langchain4j</groupId>
    <artificatId>langchain4j-alloydb-pg</artificatId>
    <version>1.0.0-beta2</version>
</dependency>
```

## AlloyDBEmbeddingStore Usage

Use a vector store to store text embedded data and perform vector search, instances of `AlloyDBEmbeddingStore` can be created by configuring provided `Builder`, it requires the following:
- `AlloyDBEngine` instance
- table name
- schema name (optional, default: "public")
- content column (optional, default: "content")
- embedding column (optional, default: "embedding")
- id column (optional, default: "langchain_id")
- metadata column names (optional)
- additional metadata json column (optional, default: "langchain_metadata")
- ignored metadata column names (optional)
- distance strategy (optional, default:DistanceStrategy.COSINE_DISTANCE)
- query options (optional).

example usage:
```java
...
    import dev.langchain4j.store.embedding.alloydb.AlloyDBEmbeddingStore;
...

    AlloyDBEmbeddingStore store = new AlloyDBEmbeddingStore.Builder(engine, TABLE_NAME)
        .build();

    List<String> testTexts = Arrays.asList("cat", "dog", "car", "truck");
    List<Embedding> embeddings = new ArrayList<>();
    List<TextSegment> textSegments = new ArrayList<>();

    for (String text : testTexts) {
        Map<String, Object> metaMap = new HashMap<>();
        metaMap.put("string_metadata", "sring");
        Metadata metadata = new Metadata(metaMap);
        textSegments.add(new TextSegment(text, metadata));
        embeddings.add(MyEmbeddingModel.embed(text).content());
    }
    List<String> ids = store.addAll(embeddings, textSegments);
    // search for "cat"
    EmbeddingSearchRequest request = EmbeddingSearchRequest.builder()
            .queryEmbedding(embeddings.get(0))
            .maxResults(10)
            .minScore(0.9)
            .build();
    List<EmbeddingMatch<TextSegment>> result = store.search(request).matches();
    // remove cat
    store.removeAll(singletonList(result.get(0).embeddingId()));

```