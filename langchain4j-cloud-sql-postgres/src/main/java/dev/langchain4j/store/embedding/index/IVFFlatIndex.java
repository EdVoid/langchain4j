package dev.langchain4j.store.embedding.index;

import java.util.List;

public class IVFFlatIndex implements BaseIndex {

  private final String indexType = "ivfflat";
  private final String name;
  private final Integer listCount;
  private final DistanceStrategy distanceStrategy;
  private final List<String> partialIndexes;

  public IVFFlatIndex(Builder builder) {
    this.name = builder.name;
    this.listCount = builder.listCount;
    this.distanceStrategy = builder.distanceStrategy;
    this.partialIndexes = builder.partialIndexes;
  }

  @Override
  public String getIndexOptions() {
    return String.format("(lists = %s)", listCount);
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
    private Integer listCount = 100;
    private DistanceStrategy distanceStrategy = DistanceStrategy.COSINE_DISTANCE;
    private List<String> partialIndexes;

    public Builder name(String name) {
      this.name = name;
      return this;
    }

    public Builder listCount(Integer listCount) {
      this.listCount = listCount;
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

    public IVFFlatIndex build() {
      return new IVFFlatIndex(this);
    }
  }
}
