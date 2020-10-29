package com.amazon.opendistroforelasticsearch.search.async.processor;

import com.amazon.opendistroforelasticsearch.search.async.AsyncSearchContext;
import com.amazon.opendistroforelasticsearch.search.async.AsyncSearchContextId;
import com.amazon.opendistroforelasticsearch.search.async.active.AsyncSearchActiveContext;
import com.amazon.opendistroforelasticsearch.search.async.active.AsyncSearchActiveStore;
import com.amazon.opendistroforelasticsearch.search.async.persistence.AsyncSearchPersistenceModel;
import com.amazon.opendistroforelasticsearch.search.async.persistence.AsyncSearchPersistenceService;
import com.amazon.opendistroforelasticsearch.search.async.plugin.AsyncSearchPlugin;
import com.amazon.opendistroforelasticsearch.search.async.response.AsyncSearchResponse;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.common.unit.TimeValue;

import java.io.IOException;
import java.util.Objects;
import java.util.Optional;

/**
 * Performs the post processing after a search completes.
 */
public class AsyncSearchPostProcessor {

    private static final Logger logger = LogManager.getLogger(AsyncSearchPostProcessor.class);

    private final AsyncSearchPersistenceService asyncSearchPersistenceService;
    private final AsyncSearchActiveStore asyncSearchActiveStore;

    public AsyncSearchPostProcessor(AsyncSearchPersistenceService asyncSearchPersistenceService, AsyncSearchActiveStore asyncSearchActiveStore) {
        Objects.requireNonNull(asyncSearchActiveStore);
        Objects.requireNonNull(asyncSearchPersistenceService);
        this.asyncSearchActiveStore = asyncSearchActiveStore;
        this.asyncSearchPersistenceService = asyncSearchPersistenceService;
    }

    public AsyncSearchResponse processSearchFailure(Exception exception, AsyncSearchContextId asyncSearchContextId) throws IOException {
        AsyncSearchPersistenceModel persistenceModel;
        final Optional<AsyncSearchActiveContext> asyncSearchContextOptional = asyncSearchActiveStore.getContext(asyncSearchContextId);
        if (asyncSearchContextOptional.isPresent()) {
            AsyncSearchActiveContext asyncSearchContext = asyncSearchContextOptional.get();
            asyncSearchContext.processSearchFailure(exception);
            if (asyncSearchContext.shouldPersist()) {
                persistenceModel = new AsyncSearchPersistenceModel(asyncSearchContext.getStartTimeMillis(), asyncSearchContext.getExpirationTimeMillis(),
                        exception);
                postProcess(asyncSearchContext, persistenceModel);
            }
            return asyncSearchContext.getAsyncSearchResponse();
        }
        return null;
    }

    public AsyncSearchResponse processSearchResponse(SearchResponse searchResponse, AsyncSearchContextId asyncSearchContextId) throws IOException {
        AsyncSearchPersistenceModel persistenceModel;
        final Optional<AsyncSearchActiveContext> asyncSearchContextOptional = asyncSearchActiveStore.getContext(asyncSearchContextId);
        if (asyncSearchContextOptional.isPresent()) {
            AsyncSearchActiveContext asyncSearchContext = asyncSearchContextOptional.get();
            asyncSearchContext.processSearchResponse(searchResponse);
            if (asyncSearchContext.shouldPersist()) {
                persistenceModel = new AsyncSearchPersistenceModel(asyncSearchContext.getStartTimeMillis(), asyncSearchContext.getExpirationTimeMillis(),
                        searchResponse);
                postProcess(asyncSearchContext, persistenceModel);
            }
            return asyncSearchContext.getAsyncSearchResponse();
        }
        return null;
    }

    private void postProcess(AsyncSearchActiveContext asyncSearchContext, AsyncSearchPersistenceModel persistenceModel) {
        //assert we are not post processing on any other thread pool
        assert Thread.currentThread().getName().contains(AsyncSearchPlugin.OPEN_DISTRO_ASYNC_SEARCH_GENERIC_THREAD_POOL_NAME);
        assert asyncSearchContext.retainedStages().contains(asyncSearchContext.getAsyncSearchStage()) : "found stage "+ asyncSearchContext.getAsyncSearchStage() + "that shouldn't be retained";
        // acquire all permits non-blocking
        asyncSearchContext.acquireAllContextPermits(ActionListener.wrap(releasable -> {
            // check again after acquiring permit if the context has been deleted mean while
            if (asyncSearchContext.getAsyncSearchStage() == AsyncSearchContext.Stage.DELETED) {
                logger.debug("Async search context has been moved to "+ asyncSearchContext.getAsyncSearchStage() + "while waiting to acquire permits for post processing");
                return;
            }
                asyncSearchPersistenceService.storeResponse(asyncSearchContext.getAsyncSearchId(), persistenceModel, ActionListener.wrap(
                        (indexResponse) -> {
                            //Mark any dangling reference as PERSISTED and cleaning it up from the IN_MEMORY context
                            asyncSearchContext.advanceStage(AsyncSearchContext.Stage.PERSISTED);
                            // Clean this up so that new context find results in a resolution from persistent store
                            asyncSearchActiveStore.freeContext(asyncSearchContext.getContextId());
                            releasable.close();
                        },

                        (e) -> {
                            asyncSearchContext.advanceStage(AsyncSearchContext.Stage.PERSIST_FAILED);
                            //TODO should we wait or retry after some time or after an event
                            logger.error("Failed to persist final response for {}", asyncSearchContext.getAsyncSearchId(), e);
                            releasable.close();
                        }
                ));

                }, (e) -> logger.error(() -> new ParameterizedMessage("Exception while acquiring the permit for asyncSearchContext {} due to ",
                        asyncSearchContext), e)),
                TimeValue.timeValueSeconds(60), "persisting response");
    }
}
