package dev.langchain4j.store.embedding.alloydb.index;

public interface VectorIndex {

    public String generateCreateIndexQuery();

    public String generateParameterSetting();
    
}