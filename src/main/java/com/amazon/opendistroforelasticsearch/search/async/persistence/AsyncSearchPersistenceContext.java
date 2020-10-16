package com.amazon.opendistroforelasticsearch.search.async.persistence;

import com.amazon.opendistroforelasticsearch.search.async.AsyncSearchContext;
import com.amazon.opendistroforelasticsearch.search.async.AsyncSearchId;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.common.xcontent.LoggingDeprecationHandler;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;

import java.io.IOException;

public class AsyncSearchPersistenceContext extends AsyncSearchContext {

    private final AsyncSearchId asyncSearchId;
    private final AsyncSearchPersistenceModel asyncSearchPersistenceModel;

    public AsyncSearchPersistenceContext(AsyncSearchId asyncSearchId, AsyncSearchPersistenceModel asyncSearchPersistenceModel) {
        super(asyncSearchId.getAsyncSearchContextId());
        this.asyncSearchId = asyncSearchId;
        this.asyncSearchPersistenceModel = asyncSearchPersistenceModel;
    }

    public AsyncSearchPersistenceModel getAsyncSearchPersistenceModel() {
        return asyncSearchPersistenceModel;
    }

    @Override
    public AsyncSearchId getAsyncSearchId() {
        return asyncSearchId;
    }

    @Override
    public boolean isRunning() {
        return false;
    }

    @Override
    public long getExpirationTimeMillis() {
        return asyncSearchPersistenceModel.getExpirationTimeMillis();
    }

    @Override
    public long getStartTimeMillis() {
        return asyncSearchPersistenceModel.getStartTimeMillis();
    }

    @Override
    public SearchResponse getSearchResponse() {
        try (XContentParser parser = XContentType.JSON.xContent().createParser(NamedXContentRegistry.EMPTY,
                LoggingDeprecationHandler.INSTANCE, asyncSearchPersistenceModel.getResponse().streamInput())) {
            return SearchResponse.fromXContent(parser);
        } catch (IOException e) {
            return null;
        }
    }

    @Override
    public Source getSource() {
        return Source.STORE;
    }
}
