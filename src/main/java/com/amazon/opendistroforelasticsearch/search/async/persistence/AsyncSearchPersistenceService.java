package com.amazon.opendistroforelasticsearch.search.async.persistence;

import com.amazon.opendistroforelasticsearch.search.async.AsyncSearchResponse;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.ResourceAlreadyExistsException;
import org.elasticsearch.action.ActionListener;
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
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.OriginSettingClient;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.MappingMetadata;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.NamedWriteableAwareStreamInput;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.settings.Setting;
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
import java.nio.ByteBuffer;
import java.util.Base64;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import static org.elasticsearch.action.admin.cluster.node.tasks.get.GetTaskAction.TASKS_ORIGIN;
import static org.elasticsearch.common.unit.TimeValue.*;

/**
 * Service that can persist async search results to disk, evict them on expiration and retrieve async search responses
 * as and when requested by the user.
 */
public class AsyncSearchPersistenceService {

    private static final Logger logger = LogManager.getLogger(AsyncSearchPersistenceService.class);

    private static final String RESPONSE_PROPERTY_NAME = "response";

    static final String EXPIRATION_TIME_PROPERTY_NAME = "expiration_time";

    private static final String ID_PROPERTY_NAME = "id";

    static final String INDEX = ".async_search_response";

    private static final String TASK_TYPE = "task";

    private static final String ASYNC_SEARCH_RESPONSE_MAPPING_VERSION_META_FIELD = "version";

    private static final int ASYNC_SEARCH_RESPONSE_MAPPING_VERSION = 3;

    /**
     * The backoff policy to use when saving a task result fails. The total wait
     * time is 600000 milliseconds, ten minutes.
     */
    static final BackoffPolicy STORE_BACKOFF_POLICY =
            BackoffPolicy.exponentialBackoff(timeValueMillis(250), 14);

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

    public void createResponseAsync(AsyncSearchResponse asyncSearchResponse, ActionListener<IndexResponse> listener) {
        threadPool.generic().submit(() -> {
                    try {
                        AsyncSearchPersistenceService.this.createResponse(asyncSearchResponse, listener);
                    } catch (IOException e) {
                        listener.onFailure(e);
                    }
                },
                ThreadPool.Names.SAME);
    }

    public void createResponse(AsyncSearchResponse asyncSearchResponse, ActionListener<IndexResponse> listener)
            throws IOException {
        ClusterState state = clusterService.state();

        if (!state.routingTable().hasIndex(INDEX)) {
            CreateIndexRequest createIndexRequest = new CreateIndexRequest();
            createIndexRequest.settings(taskResultIndexSettings());
            createIndexRequest.index(INDEX);
            createIndexRequest.mapping(TASK_TYPE, mappingSource());
            createIndexRequest.cause("auto(task api)");

            client.admin().indices().create(createIndexRequest, new ActionListener<CreateIndexResponse>() {
                @Override
                public void onResponse(CreateIndexResponse result) {
                    doStoreResult(asyncSearchResponse, listener);
                }

                @Override
                public void onFailure(Exception e) {
                    if (ExceptionsHelper.unwrapCause(e) instanceof ResourceAlreadyExistsException) {
                        // we have the index, do it
                        try {
                            doStoreResult(asyncSearchResponse, listener);
                        } catch (Exception inner) {
                            inner.addSuppressed(e);
                            listener.onFailure(inner);
                        }
                    } else {
                        listener.onFailure(e);
                    }
                }
            });
        } else {
            IndexMetadata metaData = state.getMetadata().index(INDEX);
            if (getTaskResultMappingVersion(metaData) < ASYNC_SEARCH_RESPONSE_MAPPING_VERSION) {
                // The index already exists but doesn't have our mapping
                client.admin().indices().preparePutMapping(INDEX).setType(TASK_TYPE)
                        .setSource(mappingSource(), XContentType.JSON)
                        .execute(ActionListener.delegateFailure(listener, (l, r) -> doStoreResult(asyncSearchResponse,
                                listener)));
            } else {
                doStoreResult(asyncSearchResponse, listener);
            }
        }
    }

    public void getResponse(String id, ActionListener<AsyncSearchResponse> actionListener) {
        GetRequest getRequest = new GetRequest(INDEX)
                .id(String.valueOf(id));
        client.get(getRequest, new ActionListener<GetResponse>() {
            @Override
            public void onResponse(GetResponse getResponse) {
                if (getResponse.isExists()
                        && getResponse.getSource() != null
                        && getResponse.getSource().containsKey(AsyncSearchPersistenceService.RESPONSE_PROPERTY_NAME)
                        && getResponse.getSource().containsKey(AsyncSearchPersistenceService.EXPIRATION_TIME_PROPERTY_NAME)) {
                    AsyncSearchResponse response = parseResponse((String)
                            getResponse.getSource().get(AsyncSearchPersistenceService.RESPONSE_PROPERTY_NAME));
                    actionListener.onResponse(new AsyncSearchResponse(response,
                            (long) getResponse.getSource().get(AsyncSearchPersistenceService.EXPIRATION_TIME_PROPERTY_NAME)));
                }
            }

            @Override
            public void onFailure(Exception e) {
                actionListener.onFailure(e);
            }
        });
    }

    public void deleteResponse(String id) {
        DeleteRequest deleteRequest = new DeleteRequest(INDEX, id);
        client.delete(deleteRequest, new ActionListener<DeleteResponse>() {
            @Override
            public void onResponse(DeleteResponse deleteResponse) {
                logger.debug("Deleted async search {}", id);
            }

            @Override
            public void onFailure(Exception e) {
                logger.error("Failed to delete async search " + id, e);
            }
        });
    }
    public void updateExpirationTimeAsync(String id, long expirationTimeMillis) {
        threadPool.generic().execute(() -> updateExpirationTime(id, expirationTimeMillis));
    }
    public void updateExpirationTime(String id, long expirationTimeMillis) {
        Map<String, Object> source = new HashMap<>();
        source.put(EXPIRATION_TIME_PROPERTY_NAME, expirationTimeMillis);
        UpdateRequest updateRequest = new UpdateRequest(INDEX, id);
        updateRequest.doc(source, XContentType.JSON);
        client.update(updateRequest, new ActionListener<UpdateResponse>() {
            @Override
            public void onResponse(UpdateResponse updateResponse) {
            }

            @Override
            public void onFailure(Exception e) {
            }
        });
    }

    private int getTaskResultMappingVersion(IndexMetadata metaData) {
        MappingMetadata mappingMetaData = metaData.getMappings().get(TASK_TYPE);
        if (mappingMetaData == null) {
            return 0;
        }
        @SuppressWarnings("unchecked") Map<String, Object> meta = (Map<String, Object>) mappingMetaData.sourceAsMap().get("_meta");
        if (meta == null || meta.containsKey(ASYNC_SEARCH_RESPONSE_MAPPING_VERSION_META_FIELD) == false) {
            return 1; // The mapping was created before meta field was introduced
        }
        return (int) meta.get(ASYNC_SEARCH_RESPONSE_MAPPING_VERSION_META_FIELD);
    }

    private void doStoreResult(AsyncSearchResponse asyncSearchResponse, ActionListener<IndexResponse> listener) {

        Map<String, Object> source = new HashMap<>();
        try {
            source.put(RESPONSE_PROPERTY_NAME, serializeResponse(asyncSearchResponse)); //TODO find better serializations
            source.put(EXPIRATION_TIME_PROPERTY_NAME, asyncSearchResponse.getExpirationTimeMillis());
            source.put(ID_PROPERTY_NAME, asyncSearchResponse.getId());
        } catch (IOException e) {
            listener.onFailure(e);
        }
        IndexRequestBuilder index = client.prepareIndex(INDEX, TASK_TYPE,
                asyncSearchResponse.getId()).setSource(source, XContentType.JSON);
        doStoreResult(STORE_BACKOFF_POLICY.iterator(), index, listener);
    }

    private String serializeResponse(AsyncSearchResponse asyncSearchResponse) throws IOException {

        try (BytesStreamOutput out = new BytesStreamOutput()) {
            asyncSearchResponse.writeTo(out);
            byte[] bytes = BytesReference.toBytes(out.bytes());
            return Base64.getUrlEncoder().encodeToString(bytes);
        }
    }


    private AsyncSearchResponse parseResponse(String responseField) {
        try {
            BytesReference bytesReference = BytesReference.fromByteBuffer(ByteBuffer.wrap(Base64.getUrlDecoder().decode(responseField)));
            NamedWriteableAwareStreamInput wrapperStreamInput = new NamedWriteableAwareStreamInput(bytesReference.streamInput(),
                    namedWriteableRegistry);
            AsyncSearchResponse asyncSearchResponse = new AsyncSearchResponse(wrapperStreamInput);
            wrapperStreamInput.close();
            return asyncSearchResponse;
        } catch (Exception e) {
            throw new IllegalArgumentException("Cannot parse async search id", e);
        }
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

    private Settings taskResultIndexSettings() {
        return Settings.builder()
                .put(IndexMetadata.INDEX_NUMBER_OF_SHARDS_SETTING.getKey(), 1)
                .put(IndexMetadata.INDEX_AUTO_EXPAND_REPLICAS_SETTING.getKey(), "0-1")
                .put(IndexMetadata.SETTING_PRIORITY, Integer.MAX_VALUE)
                .build();
    }

    private XContentBuilder mappingSource() throws IOException {
        XContentBuilder builder = XContentFactory.contentBuilder(XContentType.JSON);
        builder.startObject()

                //meta
                .startObject("_meta")
                .field(ASYNC_SEARCH_RESPONSE_MAPPING_VERSION_META_FIELD, ASYNC_SEARCH_RESPONSE_MAPPING_VERSION)
                .endObject()
                //meta

                //props
                .startObject("properties")

                //response
                .startObject(RESPONSE_PROPERTY_NAME).field("type", "text").endObject()
                //response

                //expiry
                .startObject(EXPIRATION_TIME_PROPERTY_NAME).field("type", "long").endObject()
                //expiry

                //id
                .startObject(ID_PROPERTY_NAME).field("type", "keyword").endObject()
                //id

                .endObject()
                //props

                .endObject();

        return builder;

    }

    public void deleteExpiredResponses(ActionListener<BulkByScrollResponse> listener) {
        if (clusterService.state().routingTable().hasIndex(INDEX)) {
            logger.info("Delete expired responses which are indexed from node [{}]",
                    clusterService.localNode().getId());
            DeleteByQueryRequest request = new DeleteByQueryRequest(INDEX);
            request.setQuery(QueryBuilders.rangeQuery(EXPIRATION_TIME_PROPERTY_NAME).lte(System.currentTimeMillis()));
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
        } else {
            logger.info("Async search index not yet created! Nothing to delete.");
            listener.onResponse(null);
        }

    }

    public void deleteResponseAsync(String id) {
        threadPool.generic().execute(() -> deleteResponse(id));
    }
}
