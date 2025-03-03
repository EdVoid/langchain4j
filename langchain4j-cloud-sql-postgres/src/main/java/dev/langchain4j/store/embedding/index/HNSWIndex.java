package dev.langchain4j.store.embedding.index;

import java.util.List;

public class HNSWIndex implements BaseIndex {

  private final String indexType = "hnsw";
  private final String name;
  private final Integer m;
  private final Integer efConstruction;
  private final DistanceStrategy distanceStrategy;
  private final List<String> partialIndexes;

  public HNSWIndex(Builder builder) {
    this.name = builder.name;
    this.m = builder.m;
    this.efConstruction = builder.efConstruction;
    this.distanceStrategy = builder.distanceStrategy;
    this.partialIndexes = builder.partialIndexes;
  }

  @Override
  public String getIndexOptions() {
    return String.format("(m = %s, ef_construction = %s)", m, efConstruction);
  }

  public DistanceStrategy getDistanceStrategy() {
    return distanceStrategy;
  }

  public List<String> getPartialIndexes() {
    return partialIndexes;
  }

  public String getIndexType() {
    return indexType;
  }

  public String getName() {
    return name;
  }

  public class Builder {

    private String name;
    private Integer m = 16;
    private Integer efConstruction = 64;
    private DistanceStrategy distanceStrategy = DistanceStrategy.COSINE_DISTANCE;
    private List<String> partialIndexes;

    public Builder m(Integer m) {
      this.m = m;
      return this;
    }

    public Builder efConstruction(Integer efConstruction) {
      this.efConstruction = efConstruction;
      return this;
    }

    public Builder distanceStrategy(DistanceStrategy distanceStrategy) {
      this.distanceStrategy = distanceStrategy;
      return this;
    }

    public Builder partialIndexes(List<String> partialIndexes) {
      this.partialIndexes = partialIndexes;
      return this;
    }

    public Builder name(String name) {
      this.name = name;
      return this;
    }

    public HNSWIndex build() {
      return new HNSWIndex(this);
    }
  }
}
