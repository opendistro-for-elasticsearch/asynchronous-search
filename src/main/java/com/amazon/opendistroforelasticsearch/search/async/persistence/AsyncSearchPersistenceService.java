package com.amazon.opendistroforelasticsearch.search.async.persistence;

import com.amazon.opendistroforelasticsearch.search.async.AsyncSearchId;
import com.amazon.opendistroforelasticsearch.search.async.response.AcknowledgedResponse;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.ResourceAlreadyExistsException;
import org.elasticsearch.ResourceNotFoundException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.DocWriteResponse;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.action.bulk.BackoffPolicy;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.Requests;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.util.concurrent.EsRejectedExecutionException;
import org.elasticsearch.common.xcontent.LoggingDeprecationHandler;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.reindex.DeleteByQueryAction;
import org.elasticsearch.index.reindex.DeleteByQueryRequest;
import org.elasticsearch.threadpool.ThreadPool;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import static com.amazon.opendistroforelasticsearch.search.async.persistence.AsyncSearchPersistenceModel.EXPIRATION_TIME_MILLIS;
import static com.amazon.opendistroforelasticsearch.search.async.persistence.AsyncSearchPersistenceModel.RESPONSE;
import static com.amazon.opendistroforelasticsearch.search.async.persistence.AsyncSearchPersistenceModel.START_TIME_MILLIS;
import static org.elasticsearch.common.unit.TimeValue.timeValueMillis;

public class AsyncSearchPersistenceService {

    private static final Logger logger = LogManager.getLogger(AsyncSearchPersistenceService.class);

    private static final String ASYNC_SEARCH_RESPONSE_INDEX_NAME = ".async_search_response";

    private static final String MAPPING_TYPE = "_doc";

    /**
     * The backoff policy to use when saving a task result fails. The total wait
     * time is 600000 milliseconds, ten minutes.
     */
    private static final BackoffPolicy STORE_BACKOFF_POLICY = BackoffPolicy.exponentialBackoff(timeValueMillis(250), 14);

    private final Client client;
    private final ClusterService clusterService;
    private final ThreadPool threadPool;
    private final NamedXContentRegistry xContentRegistry;

    @Inject public AsyncSearchPersistenceService(
        Client client, ClusterService clusterService, ThreadPool threadPool, NamedXContentRegistry xContentRegistry) {
        this.client = client;
        this.clusterService = clusterService;
        this.threadPool = threadPool;
        this.xContentRegistry = xContentRegistry;
    }

    /**
     * Creates async search response as document in index. Creates index if necessary, before creating document. Retries response
     * creation on failure with exponential backoff
     */
    public void createResponse(AsyncSearchPersistenceContext model, ActionListener<IndexResponse> listener) {
        if (indexExists()) {
            doStoreResult(model, listener);
        } else {
            createIndexAndDoStoreResult(model, listener);
        }
    }

    /**
     * Throws ResourceNotFoundException if index doesn't exist
     */
    public void getResponse(String id, ActionListener<AsyncSearchPersistenceContext> listener) {
        if (!indexExists()) {
            listener.onFailure(new ResourceNotFoundException(id));
        }

        client.get(new GetRequest(ASYNC_SEARCH_RESPONSE_INDEX_NAME, id),
            ActionListener.wrap(getResponse -> parseResponse(id, getResponse, listener), exception -> {
                logger.error("Failed to get response for async search [" + id + "]", exception);
                listener.onFailure(exception);
            }));
    }

    public void deleteResponse(String id, ActionListener<Boolean> listener) {
        if (!indexExists()) {
            listener.onResponse(false);
            return;
        }

        client.delete(new DeleteRequest(ASYNC_SEARCH_RESPONSE_INDEX_NAME, id), ActionListener.wrap(deleteResponse -> {
            if (deleteResponse.getResult() == DocWriteResponse.Result.DELETED) {
                logger.debug("Deleted async search {}", id);
                listener.onResponse(true);
            } else {
                logger.debug("Delete async search {} unsuccessful. Returned result {}", id, deleteResponse.getResult());
                listener.onResponse(false);
            }
        }, e -> {
            logger.error("Failed to delete async search " + id, e);
            listener.onFailure(e);
        }));
    }

    /**
     * Throws ResourceNotFoundException if index doesn't exist.
     */

    public void updateExpirationTime(String id, long expirationTimeMillis, ActionListener<ActionResponse> listener) {
        if (!indexExists()) {
            listener.onFailure(new ResourceNotFoundException(id));
        }
        Map<String, Object> source = new HashMap<>();
        source.put(EXPIRATION_TIME_MILLIS, expirationTimeMillis);
        UpdateRequest updateRequest = new UpdateRequest(ASYNC_SEARCH_RESPONSE_INDEX_NAME, id);
        updateRequest.doc(source, XContentType.JSON);
        client.update(updateRequest, ActionListener.wrap(
                updateResponse -> {
                    if (updateResponse.getResult() == DocWriteResponse.Result.UPDATED) {
                        listener.onResponse(null);
                    } else if (updateResponse.getResult() == DocWriteResponse.Result.NOT_FOUND) {
                        listener.onFailure(new ResourceNotFoundException(id));
                    } else {
                        listener.onFailure(new IOException("Failed to update keep_alive for async search " + id));
                    }
                },
                exception -> {
                    listener.onFailure(new IOException("Failed to update keep_alive for async search " + id));
                }));

    }

    public void deleteExpiredResponses(ActionListener<AcknowledgedResponse> listener) {
        if (!indexExists()) {
            logger.info("Async search index not yet created! Nothing to delete.");
            listener.onResponse(new AcknowledgedResponse(true));
        } else {
            logger.info("Deleting expired async search responses");
            DeleteByQueryRequest request =
                    new DeleteByQueryRequest(ASYNC_SEARCH_RESPONSE_INDEX_NAME)
                            .setQuery(QueryBuilders.rangeQuery(EXPIRATION_TIME_MILLIS)
                                    .lte(System.currentTimeMillis()));
            client.execute(DeleteByQueryAction.INSTANCE, request, ActionListener.wrap(
                    r -> listener.onResponse(new AcknowledgedResponse(true)),
                    listener::onFailure));
        }

    }

    private void createIndexAndDoStoreResult(AsyncSearchPersistenceContext model, ActionListener<IndexResponse> listener) {
        createAsyncSearchResponseIndex(ActionListener.wrap(createIndexResponse -> doStoreResult(model, listener), exception -> {
            if (ExceptionsHelper.unwrapCause(exception) instanceof ResourceAlreadyExistsException) {
                // we have the index, do it
                try {
                    doStoreResult(model, listener);
                } catch (Exception inner) {
                    inner.addSuppressed(exception);
                    listener.onFailure(inner);
                }
            } else {
                listener.onFailure(exception);
            }
        }));
    }

    private void createAsyncSearchResponseIndex(ActionListener<CreateIndexResponse> listener) {
        CreateIndexRequest
            createIndexRequest =
            new CreateIndexRequest().mapping(MAPPING_TYPE, mapping())
                .settings(indexSettings())
                .index(ASYNC_SEARCH_RESPONSE_INDEX_NAME)
                .cause("async_search_response_index");
        client.admin().indices().create(createIndexRequest, listener);
    }

    private void doStoreResult(AsyncSearchPersistenceContext context, ActionListener<IndexResponse> listener) {

        IndexRequestBuilder
            index =
            client.prepareIndex(ASYNC_SEARCH_RESPONSE_INDEX_NAME, MAPPING_TYPE, AsyncSearchId.buildAsyncId(context.getAsyncSearchId()));

        try (XContentBuilder builder = XContentFactory.contentBuilder(Requests.INDEX_CONTENT_TYPE)) {
            context.getAsyncSearchPersistenceModel().toXContent(builder, ToXContent.EMPTY_PARAMS);
            index.setSource(builder);
        } catch (IOException e) {
            throw new ElasticsearchException("Couldn't convert async search persistence context to XContent for [{}]",
                e,
                context.getAsyncSearchPersistenceModel());
        }
        doStoreResult(STORE_BACKOFF_POLICY.iterator(), index, listener);
    }

    private void doStoreResult(Iterator<TimeValue> backoff, IndexRequestBuilder index, ActionListener<IndexResponse> listener) {
        index.execute(new ActionListener<IndexResponse>() {
            @Override public void onResponse(IndexResponse indexResponse) {
                listener.onResponse(indexResponse);
            }

            @Override public void onFailure(Exception e) {
                if (!(e instanceof EsRejectedExecutionException) || !backoff.hasNext()) {
                    listener.onFailure(e);
                } else {
                    TimeValue wait = backoff.next();
                    logger.warn(() -> new ParameterizedMessage("failed to store async search response, retrying in [{}]", wait), e);
                    threadPool.schedule(() -> doStoreResult(backoff, index, listener), wait, ThreadPool.Names.SAME);
                }
            }
        });
    }

    private Settings indexSettings() {
        return Settings.builder()
            .put(IndexMetadata.INDEX_NUMBER_OF_SHARDS_SETTING.getKey(), 1)
            .put(IndexMetadata.INDEX_AUTO_EXPAND_REPLICAS_SETTING.getKey(), "0-1")
            .put(IndexMetadata.SETTING_PRIORITY, Integer.MAX_VALUE)
            .build();
    }

    private XContentBuilder mapping() {
        try {

            XContentBuilder builder = XContentFactory.contentBuilder(XContentType.JSON);
            builder.startObject()

                //props
                .startObject("properties")

                //expiry
                .startObject(START_TIME_MILLIS).field("type", "long").endObject()
                //expiry

                //expiry
                .startObject(EXPIRATION_TIME_MILLIS).field("type", "long").endObject()
                //expiry

                //response
                .startObject(RESPONSE).field("type", "object").endObject()
                //response

                .endObject()
                //props

                .endObject();

            return builder;
        } catch (IOException e) {
            throw new IllegalArgumentException("Async search persistence mapping cannot be read correctly.", e);
        }

    }

    private boolean indexExists() {
        return clusterService.state().routingTable().hasIndex(ASYNC_SEARCH_RESPONSE_INDEX_NAME);
    }

    void parseResponse(String id, GetResponse getResponse, ActionListener<AsyncSearchPersistenceContext> listener) {
        if (getResponse.isExists() && isIndexedResponseExpired(getResponse) ==  false) {
            try {
                XContentParser parser = XContentHelper.createParser(NamedXContentRegistry.EMPTY,
                    LoggingDeprecationHandler.INSTANCE,
                    getResponse.getSourceAsBytesRef(),
                    Requests.INDEX_CONTENT_TYPE);
                listener.onResponse(new AsyncSearchPersistenceContext(AsyncSearchId.parseAsyncId(id),
                    AsyncSearchPersistenceModel.PARSER.apply(parser, null)));
            } catch (IOException e) {
                logger.error("IOException occurred finding lock", e);
                listener.onFailure(new ResourceNotFoundException(id));
            }
        } else {
            listener.onFailure(new ResourceNotFoundException(id));
        }
    }

    private boolean isIndexedResponseExpired(GetResponse getResponse) {
        return getResponse.getSource().containsKey(EXPIRATION_TIME_MILLIS) && (long) getResponse.getSource()
            .get(EXPIRATION_TIME_MILLIS) < System.currentTimeMillis();
    }
}
