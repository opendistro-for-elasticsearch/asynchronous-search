package com.amazon.opendistroforelasticsearch.search.async.persistence;

import com.amazon.opendistroforelasticsearch.search.async.AsyncSearchId;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
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
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.OriginSettingClient;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.util.concurrent.EsRejectedExecutionException;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.reindex.BulkByScrollResponse;
import org.elasticsearch.index.reindex.DeleteByQueryAction;
import org.elasticsearch.index.reindex.DeleteByQueryRequest;
import org.elasticsearch.threadpool.ThreadPool;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import static com.amazon.opendistroforelasticsearch.search.async.persistence.AsyncSearchPersistenceModel.EXPIRATION_TIME;
import static com.amazon.opendistroforelasticsearch.search.async.persistence.AsyncSearchPersistenceModel.RESPONSE;
import static org.elasticsearch.action.admin.cluster.node.tasks.get.GetTaskAction.TASKS_ORIGIN;
import static org.elasticsearch.common.unit.TimeValue.timeValueMillis;

public class AsyncSearchPersistenceService {

    private static final Logger logger = LogManager.getLogger(AsyncSearchPersistenceService.class);

    private static final String ASYNC_SEARCH_RESPONSE_INDEX_NAME = ".async_search_response";

    private static final String MAPPING_TYPE = "_doc";

    /**
     * The backoff policy to use when saving a task result fails. The total wait
     * time is 600000 milliseconds, ten minutes.
     */
    private static final BackoffPolicy STORE_BACKOFF_POLICY =
            BackoffPolicy.exponentialBackoff(timeValueMillis(250), 14);
    public static final String ASYNC_SEARCH_RESPONSE_MAPPING_FILE = "opendistro_async_search_response_index_mapping.json";

    private final Client client;
    private final ClusterService clusterService;
    private final ThreadPool threadPool;
    private final NamedWriteableRegistry namedWriteableRegistry;


    @Inject
    public AsyncSearchPersistenceService(Client client, ClusterService clusterService, ThreadPool threadPool,
                                         NamedWriteableRegistry namedWriteableRegistry) {
        this.namedWriteableRegistry = namedWriteableRegistry;
        this.client = new OriginSettingClient(client, TASKS_ORIGIN);
        this.clusterService = clusterService;
        this.threadPool = threadPool;
    }

    /**
     * Creates async search response as document in index. Creates index if necessary, before creating document. Retries response
     * creation on failure with exponential backoff
     */
    public void createResponse(AsyncSearchPersistenceModel model, ActionListener<IndexResponse> listener) {
        if (indexExists()) {
            doStoreResult(model, listener);
        } else {
            createIndexAndDoStoreResult(model, listener);
        }
    }

    /**
     * Throws ResourceNotFoundException if index doesn't exist
     */
    public void getResponse(String id, ActionListener<AsyncSearchPersistenceModel> listener) {
        if (!indexExists()) {
            listener.onFailure(new ResourceNotFoundException(id));
        }

        client.get(new GetRequest(ASYNC_SEARCH_RESPONSE_INDEX_NAME, id), ActionListener.wrap(
                getResponse -> processGetResponse(id, getResponse, listener),
                exception -> {
                    logger.error("Failed to get response for async search [" + id + "]", exception);
                    listener.onFailure(exception);
                }));
    }

    public void deleteResponse(String id, ActionListener<Boolean> listener) {
        if (!indexExists()) {
            listener.onFailure(new ResourceNotFoundException(id));
        }
        client.delete(new DeleteRequest(ASYNC_SEARCH_RESPONSE_INDEX_NAME, id), new ActionListener<DeleteResponse>() {
            @Override
            public void onResponse(DeleteResponse deleteResponse) {
                if (deleteResponse.getResult() == DocWriteResponse.Result.NOT_FOUND) {
                    listener.onFailure(new ResourceNotFoundException(id));
                } else {
                    logger.debug("Deleted async search {}", id);
                    listener.onResponse(true);
                }
            }

            @Override
            public void onFailure(Exception e) {
                logger.error("Failed to delete async search " + id, e);
                listener.onFailure(e);
            }
        });
    }

    /**
     * Throws ResourceNotFoundException if index doesn't exist.
     */

    public void updateExpirationTime(String id, long expirationTimeNanos, ActionListener<ActionResponse> listener) {
        if (!indexExists()) {
            listener.onFailure(new ResourceNotFoundException(id));
        }
        Map<String, Object> source = new HashMap<>();
        source.put(EXPIRATION_TIME, expirationTimeNanos);
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

    public void deleteExpiredResponses(ActionListener<BulkByScrollResponse> listener) {
        if (!indexExists()) {
            logger.info("Async search index not yet created! Nothing to delete.");
            listener.onResponse(null);
        } else {
            logger.info("Deleting expired async search responses");
            DeleteByQueryRequest request =
                    new DeleteByQueryRequest(ASYNC_SEARCH_RESPONSE_INDEX_NAME)
                            .setQuery(QueryBuilders.rangeQuery(EXPIRATION_TIME)
                                    .lte(System.nanoTime()));
            client.execute(DeleteByQueryAction.INSTANCE, request, new ActionListener<BulkByScrollResponse>() {
                @Override
                public void onResponse(BulkByScrollResponse bulkByScrollResponse) {
                    listener.onResponse(bulkByScrollResponse);
                }

                @Override
                public void onFailure(Exception e) {
                    logger.warn("Failed to perform delete by query", e);
                    listener.onFailure(e);
                }
            });
        }

    }

    private void createIndexAndDoStoreResult(AsyncSearchPersistenceModel model, ActionListener<IndexResponse> listener) {
        createAsyncSearchResponseIndex(ActionListener.wrap(
                createIndexResponse -> doStoreResult(model, listener),
                exception -> {
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
        CreateIndexRequest createIndexRequest = new CreateIndexRequest()
                .mapping(MAPPING_TYPE, mapping())
                .settings(indexSettings())
                .index(ASYNC_SEARCH_RESPONSE_INDEX_NAME)
                .cause("async_search_response_index");
        client.admin().indices().create(createIndexRequest, listener);
    }

    private void processGetResponse(String id, GetResponse getResponse, ActionListener<AsyncSearchPersistenceModel> listener) {
        if (getResponse.isExists() &&
                getResponse.getSource() != null
                && getResponse.getSource().containsKey(RESPONSE)
                && getResponse.getSource().containsKey(EXPIRATION_TIME)) {
            long expirationTimeNanos =
                    (long) getResponse.getSource().get(EXPIRATION_TIME);
            if (System.nanoTime() < expirationTimeNanos) {
                listener.onResponse(new AsyncSearchPersistenceModel(namedWriteableRegistry, AsyncSearchId.parseAsyncId(id),
                        expirationTimeNanos,
                        (String) getResponse.getSource().get(RESPONSE)));
            } else {
                listener.onFailure(new ResourceNotFoundException(id));
            }

        } else {
            listener.onFailure(new ResourceNotFoundException(id));
        }
    }

    private void doStoreResult(AsyncSearchPersistenceModel model, ActionListener<IndexResponse> listener) {

        Map<String, Object> source = new HashMap<>();
        source.put(RESPONSE, model.getResponse());
        source.put(EXPIRATION_TIME, model.getExpirationTimeNanos());
        IndexRequestBuilder index = client.prepareIndex(ASYNC_SEARCH_RESPONSE_INDEX_NAME, MAPPING_TYPE,
                AsyncSearchId.buildAsyncId(model.getAsyncSearchId())).setSource(source, XContentType.JSON);
        doStoreResult(STORE_BACKOFF_POLICY.iterator(), index, listener);
    }


    private void doStoreResult(Iterator<TimeValue> backoff, IndexRequestBuilder index, ActionListener<IndexResponse> listener) {
        index.execute(new ActionListener<IndexResponse>() {
            @Override
            public void onResponse(IndexResponse indexResponse) {
                listener.onResponse(indexResponse);
            }

            @Override
            public void onFailure(Exception e) {
                if (!(e instanceof EsRejectedExecutionException)
                        || !backoff.hasNext()) {
                    listener.onFailure(e);
                } else {
                    TimeValue wait = backoff.next();
                    logger.warn(() -> new ParameterizedMessage("failed to store task result, retrying in [{}]", wait), e);
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

                    //response
                    .startObject(RESPONSE).field("type", "text").endObject()
                    //response

                    //expiry
                    .startObject(EXPIRATION_TIME).field("type", "long").endObject()
                    //expiry


                    .endObject()
                    //props

                    .endObject();

            return builder;
        } catch (IOException e) {
            throw new IllegalArgumentException("Async search persistence mapping cannot be read correctly.");
        }

    }

    private boolean indexExists() {
        return clusterService.state().routingTable().hasIndex(ASYNC_SEARCH_RESPONSE_INDEX_NAME);
    }

}
