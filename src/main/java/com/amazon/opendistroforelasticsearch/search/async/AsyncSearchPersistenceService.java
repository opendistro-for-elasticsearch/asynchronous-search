package com.amazon.opendistroforelasticsearch.search.async;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.ResourceAlreadyExistsException;
import org.elasticsearch.ResourceNotFoundException;
import org.elasticsearch.action.ActionFuture;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.action.bulk.BackoffPolicy;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.index.IndexResponse;
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
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.util.concurrent.EsRejectedExecutionException;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.tasks.TaskResult;
import org.elasticsearch.threadpool.ThreadPool;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import static org.elasticsearch.action.admin.cluster.node.tasks.get.GetTaskAction.TASKS_ORIGIN;
import static org.elasticsearch.common.unit.TimeValue.timeValueMillis;

/**
 * Service that can store async search results.
 */
public class AsyncSearchPersistenceService {

    private static final Logger logger = LogManager.getLogger(AsyncSearchPersistenceService.class);

    private static final String RESPONSE_PROPERTY_NAME = "response";

    public static final String EXPIRATION_TIME_PROPERTY_NAME = "expiration_time";

    public static final String ID_PROPERTY_NAME = "id";

    private static final String INDEX = ".async_search_response";

    private static final String TASK_TYPE = "task";

    private static final String ASYNC_SEARCH_RESPONSE_INDEX_MAPPING_FILE = "async_search_response-index-mapping.json";

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

    //TODO add update response
    public void createResponse(TaskResult taskResult, AsyncSearchResponse asyncSearchResponse, ActionListener<IndexResponse> listener)
            throws IOException {
        ClusterState state = clusterService.state();

        if (state.routingTable().hasIndex(INDEX) == false) {
            CreateIndexRequest createIndexRequest = new CreateIndexRequest();
            createIndexRequest.settings(taskResultIndexSettings());
            createIndexRequest.index(INDEX);
            createIndexRequest.mapping(TASK_TYPE, mappingSource());
            createIndexRequest.cause("auto(task api)");

            client.admin().indices().create(createIndexRequest, new ActionListener<CreateIndexResponse>() {
                @Override
                public void onResponse(CreateIndexResponse result) {
                    doStoreResult(taskResult, asyncSearchResponse, listener);
                }

                @Override
                public void onFailure(Exception e) {
                    if (ExceptionsHelper.unwrapCause(e) instanceof ResourceAlreadyExistsException) {
                        // we have the index, do it
                        try {
                            doStoreResult(taskResult, asyncSearchResponse, listener);
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
                        .execute(ActionListener.delegateFailure(listener, (l, r) -> doStoreResult(taskResult,asyncSearchResponse,
                                listener)));
            } else {
                doStoreResult(taskResult, asyncSearchResponse, listener);
            }
        }
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

    private void doStoreResult(TaskResult taskResult, AsyncSearchResponse asyncSearchResponse, ActionListener<IndexResponse> listener) {

        Map<String, Object> source = new HashMap<>();
        try {
            source.put(RESPONSE_PROPERTY_NAME, serializeResponse(asyncSearchResponse)); //TODO find better serializations
        } catch (IOException e) {
            listener.onFailure(e);
        }
        source.put(EXPIRATION_TIME_PROPERTY_NAME, asyncSearchResponse.getExpirationTimeMillis());
        source.put(ID_PROPERTY_NAME, asyncSearchResponse.getId());
        IndexRequestBuilder index = client.prepareIndex(INDEX, TASK_TYPE,
                asyncSearchResponse.getId()).setSource(source, XContentType.JSON);
        doStoreResult(STORE_BACKOFF_POLICY.iterator(), index, listener);
    }

    private BytesReference serializeResponse(AsyncSearchResponse asyncSearchResponse) throws IOException {

        //FIXME : Here I create an output stream. write response to it i.e. serialization and try to create response
        // from the bytes of the output stream's input (deserialization). Need to achieve the same result from whatever we index in the doc
        // be it as string or bytes.

        try(BytesStreamOutput out = new BytesStreamOutput()) {
            asyncSearchResponse.writeTo(out);
            AsyncSearchResponse deserialized = new AsyncSearchResponse(
                    new NamedWriteableAwareStreamInput(out.bytes().streamInput(), namedWriteableRegistry));
            assert deserialized.getId().equals(asyncSearchResponse.getId());
        }

        try(BytesStreamOutput out = new BytesStreamOutput()) {
            asyncSearchResponse.writeTo(out);
            return out.bytes();
        }
    }


    public AsyncSearchResponse parseResponse(BytesReference bytesReference) {
        try {
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

    public XContentBuilder mappingSource() throws IOException {
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
                .startObject(EXPIRATION_TIME_PROPERTY_NAME).field("type", "keyword").endObject()
                //expiry

                //id
                .startObject(ID_PROPERTY_NAME).field("type", "keyword").endObject()
                //id

                .endObject()
                //props

                .endObject();

        return builder;

    }

    public AsyncSearchResponse getResponse(String id) throws IOException {
        GetRequest getRequest = new GetRequest(INDEX)
                .id(String.valueOf(id));
        ActionFuture<GetResponse> future = client.get(getRequest);
        GetResponse getResponse = future.actionGet();
        if (getResponse.isExists()
                && getResponse.getSource() != null
                && getResponse.getSource().containsKey(RESPONSE_PROPERTY_NAME)
                && getResponse.getSource().containsKey(EXPIRATION_TIME_PROPERTY_NAME)) {

            BytesReference bytesReference = (BytesReference) getResponse.getSource().get(RESPONSE_PROPERTY_NAME);

            long expirationTime = (Long) getResponse.getSource().get(EXPIRATION_TIME_PROPERTY_NAME);
            if (expirationTime > System.currentTimeMillis()) {
                return parseResponse(bytesReference);
            }
        }
        throw new ResourceNotFoundException("not found");
    }
}
