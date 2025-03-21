package dev.langchain4j.model.jina;

import static dev.langchain4j.internal.RetryUtils.withRetry;
import static dev.langchain4j.internal.Utils.getOrDefault;
import static dev.langchain4j.internal.ValidationUtils.ensureNotBlank;
import static java.time.Duration.ofSeconds;
import static java.util.stream.Collectors.toList;

import dev.langchain4j.data.embedding.Embedding;
import dev.langchain4j.data.segment.TextSegment;
import dev.langchain4j.model.embedding.DimensionAwareEmbeddingModel;
import dev.langchain4j.model.embedding.EmbeddingModel;
import dev.langchain4j.model.jina.internal.api.JinaEmbeddingRequest;
import dev.langchain4j.model.jina.internal.api.JinaEmbeddingResponse;
import dev.langchain4j.model.jina.internal.client.JinaClient;
import dev.langchain4j.model.output.Response;
import dev.langchain4j.model.output.TokenUsage;
import java.time.Duration;
import java.util.List;
import lombok.Builder;

/**
 * An implementation of an {@link EmbeddingModel} that uses
 * <a href="https://jina.ai/embeddings">Jina Embeddings API</a>.
 */
public class JinaEmbeddingModel extends DimensionAwareEmbeddingModel {

    private static final String DEFAULT_BASE_URL = "https://api.jina.ai/";

    private final JinaClient client;
    private final String modelName;
    private final Integer maxRetries;
    private final Boolean lateChunking;

    @Builder
    public JinaEmbeddingModel(
            String baseUrl,
            String apiKey,
            String modelName,
            Duration timeout,
            Integer maxRetries,
            Boolean lateChunking,
            Boolean logRequests,
            Boolean logResponses) {
        this.client = JinaClient.builder()
                .baseUrl(getOrDefault(baseUrl, DEFAULT_BASE_URL))
                .apiKey(apiKey)
                .timeout(getOrDefault(timeout, ofSeconds(60)))
                .logRequests(getOrDefault(logRequests, false))
                .logResponses(getOrDefault(logResponses, false))
                .build();
        this.modelName = ensureNotBlank(modelName, "modelName");
        this.maxRetries = getOrDefault(maxRetries, 3);
        this.lateChunking = getOrDefault(lateChunking, false);
    }

    @Override
    public Response<List<Embedding>> embedAll(List<TextSegment> textSegments) {

        JinaEmbeddingRequest request = JinaEmbeddingRequest.builder()
                .model(modelName)
                .lateChunking(lateChunking)
                .input(textSegments.stream().map(TextSegment::text).collect(toList()))
                .build();

        JinaEmbeddingResponse response = withRetry(() -> client.embed(request), maxRetries);

        List<Embedding> embeddings = response.data.stream()
                .map(jinaEmbedding -> Embedding.from(jinaEmbedding.embedding))
                .collect(toList());

        TokenUsage tokenUsage = new TokenUsage(response.usage.promptTokens, 0, response.usage.totalTokens);
        return Response.from(embeddings, tokenUsage);
    }
}
