package com.amazon.opendistroforelasticsearch.search.async.processor;

import com.amazon.opendistroforelasticsearch.search.async.AsyncSearchContext;
import com.amazon.opendistroforelasticsearch.search.async.AsyncSearchContextId;
import com.amazon.opendistroforelasticsearch.search.async.AsyncSearchId;
import com.amazon.opendistroforelasticsearch.search.async.active.AsyncSearchActiveContext;
import com.amazon.opendistroforelasticsearch.search.async.active.AsyncSearchActiveStore;
import com.amazon.opendistroforelasticsearch.search.async.persistence.AsyncSearchPersistenceModel;
import com.amazon.opendistroforelasticsearch.search.async.persistence.AsyncSearchPersistenceService;
import com.amazon.opendistroforelasticsearch.search.async.response.AsyncSearchResponse;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.threadpool.ThreadPool;

import java.util.Optional;

public class AsyncSearchPostProcessor {

    private static final Logger logger = LogManager.getLogger(AsyncSearchPostProcessor.class);
    private final AsyncSearchPersistenceService asyncSearchPersistenceService;
    private final AsyncSearchActiveStore asyncSearchActiveStore;
    private final ThreadPool threadPool;

    public AsyncSearchPostProcessor(AsyncSearchPersistenceService asyncSearchPersistenceService, AsyncSearchActiveStore asyncSearchActiveStore,
                                    ThreadPool threadPool) {
        this.asyncSearchActiveStore = asyncSearchActiveStore;
        this.asyncSearchPersistenceService = asyncSearchPersistenceService;
        this.threadPool = threadPool;
    }

    public void postProcessSearchFailure(Exception exception, AsyncSearchContextId asyncSearchContextId) {
        final AsyncSearchActiveContext asyncSearchContext = asyncSearchActiveStore.getContext(asyncSearchContextId);
        asyncSearchContext.processSearchFailure(exception);
        if (asyncSearchContext.shouldPersist()) {
            postProcess(asyncSearchContext, Optional.empty(), Optional.of(exception));
        }
    }

    public AsyncSearchResponse postProcessSearchResponse(SearchResponse searchResponse, AsyncSearchContextId asyncSearchContextId) {
        final AsyncSearchActiveContext asyncSearchContext = asyncSearchActiveStore.getContext(asyncSearchContextId);
        asyncSearchContext.processSearchSuccess(searchResponse);
       if (asyncSearchContext.shouldPersist()) {
           postProcess(asyncSearchContext, Optional.of(searchResponse), Optional.empty());
        }
        return asyncSearchContext.getAsyncSearchResponse();
    }

    private void postProcess(AsyncSearchActiveContext asyncSearchContext, Optional<SearchResponse> searchResponse, Optional<Exception> exception) {
        asyncSearchContext.acquireAllContextPermits(ActionListener.wrap(releasable -> {
                    AsyncSearchPersistenceModel persistenceModel = null;
                    if (exception.isPresent()) {
                        persistenceModel = new AsyncSearchPersistenceModel(asyncSearchContext.getStartTimeMillis(), asyncSearchContext.getExpirationTimeMillis(),
                                exception.get());
                    } else if (searchResponse.isPresent()){
                        persistenceModel = new AsyncSearchPersistenceModel(asyncSearchContext.getStartTimeMillis(), asyncSearchContext.getExpirationTimeMillis(),
                                searchResponse.get());
                    }
                    asyncSearchPersistenceService.storeResponse(AsyncSearchId.buildAsyncId(asyncSearchContext.getAsyncSearchId()), persistenceModel, ActionListener.wrap(
                            (indexResponse) -> {
                                //Mark any dangling reference as PERSISTED and cleaning it up from the IN_MEMORY context
                                asyncSearchContext.setStage(AsyncSearchContext.Stage.PERSISTED);
                                // Clean this up so that new context find results in a resolution from persistent store
                                asyncSearchActiveStore.freeContext(asyncSearchContext.getAsyncSearchContextId());
                                releasable.close();
                            },

                            (e) -> {
                                asyncSearchContext.setStage(AsyncSearchContext.Stage.PERSIST_FAILED);
                                logger.error("Failed to persist final response for {}", asyncSearchContext.getAsyncSearchId(), e);
                                releasable.close();
                            }
                    ));

                }, (e) -> logger.error("Exception while acquiring the permit due to ", e)),
                TimeValue.timeValueSeconds(60), threadPool, "persisting response");
    }
}
