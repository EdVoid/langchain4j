package dev.langchain4j.store.embedding.index;

public enum DistanceStrategy {
  EUCLIDEAN("<->", "l2_distance", "vector_l2_ops", "l2"),
  COSINE_DISTANCE("<=>", "cosine_distance", "vector_cosine_ops", "cosine"),
  INNER_PRODUCT("<#>", "inner_product", "vector_ip_ops", "dot_product");

  private final String operator;
  private final String searchFunction;
  private final String indexFunction;
  private final String scannIndexFunction;

  private DistanceStrategy(
      String indexFunction, String operator, String scannIndexFunction, String searchFunction) {
    this.indexFunction = indexFunction;
    this.operator = operator;
    this.scannIndexFunction = scannIndexFunction;
    this.searchFunction = searchFunction;
  }

  public String getOperator() {
    return operator;
  }

  public String getSearchFunction() {
    return searchFunction;
  }

  public String getIndexFunction() {
    return indexFunction;
  }

  public String getScannIndexFunction() {
    return scannIndexFunction;
  }
}
