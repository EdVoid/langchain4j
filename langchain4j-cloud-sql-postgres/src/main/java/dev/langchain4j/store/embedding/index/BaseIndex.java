package dev.langchain4j.store.embedding.index;

public interface BaseIndex {

  final String DEFAULT_INDEX_NAME_SUFFIX = "langchainvectorindex";

  public String getIndexOptions();
}
