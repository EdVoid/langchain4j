package dev.langchain4j.store.embedding.index.query;

import java.util.ArrayList;
import java.util.List;

public class HNSWQueryOptions implements QueryOptions {

  private final Integer efSearch;

  public HNSWQueryOptions(Builder builder) {
    this.efSearch = builder.efSearch;
  }

  @Override
  public List<String> getParameterSettings() {
    List<String> parameters = new ArrayList();
    parameters.add(String.format("nsw.efS_search = %d", efSearch));
    return parameters;
  }

  public class Builder {

    private Integer efSearch = 40;

    public Builder efSearch(Integer efSearch) {
      this.efSearch = efSearch;
      return this;
    }

    public HNSWQueryOptions build() {
      return new HNSWQueryOptions(this);
    }
  }
}
