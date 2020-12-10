package com.amazon.opendistroforelasticsearch.search.async.processor;

import com.amazon.opendistroforelasticsearch.search.async.context.AsyncSearchContextId;
import com.amazon.opendistroforelasticsearch.search.async.context.active.AsyncSearchActiveContext;
import com.amazon.opendistroforelasticsearch.search.async.context.persistence.AsyncSearchPersistenceModel;
import com.amazon.opendistroforelasticsearch.search.async.context.state.AsyncSearchStateMachine;
import com.amazon.opendistroforelasticsearch.search.async.context.state.event.BeginPersistEvent;
import com.amazon.opendistroforelasticsearch.search.async.context.state.event.SearchFailureEvent;
import com.amazon.opendistroforelasticsearch.search.async.context.state.event.SearchResponsePersistFailedEvent;
import com.amazon.opendistroforelasticsearch.search.async.context.state.event.SearchResponsePersistedEvent;
import com.amazon.opendistroforelasticsearch.search.async.context.state.event.SearchSuccessfulEvent;
import com.amazon.opendistroforelasticsearch.search.async.plugin.AsyncSearchPlugin;
import com.amazon.opendistroforelasticsearch.search.async.response.AsyncSearchResponse;
import com.amazon.opendistroforelasticsearch.search.async.service.active.AsyncSearchActiveStore;
import com.amazon.opendistroforelasticsearch.search.async.service.persistence.AsyncSearchPersistenceService;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.threadpool.ThreadPool;

import java.util.Optional;

/**
 * Performs the processing after a search completes.
 */
public class AsyncSearchPostProcessor {

    private static final Logger logger = LogManager.getLogger(AsyncSearchPostProcessor.class);

    private final AsyncSearchPersistenceService asyncSearchPersistenceService;
    private final AsyncSearchActiveStore asyncSearchActiveStore;
    private final AsyncSearchStateMachine asyncSearchStateMachine;
    private final ThreadPool threadPool;

    public AsyncSearchPostProcessor(AsyncSearchPersistenceService asyncSearchPersistenceService,
                                    AsyncSearchActiveStore asyncSearchActiveStore, AsyncSearchStateMachine stateMachine,
                                    ThreadPool threadPool) {
        this.asyncSearchActiveStore = asyncSearchActiveStore;
        this.asyncSearchPersistenceService = asyncSearchPersistenceService;
        this.asyncSearchStateMachine = stateMachine;
        this.threadPool = threadPool;
    }

    public AsyncSearchResponse processSearchFailure(Exception exception, AsyncSearchContextId asyncSearchContextId) {
        final Optional<AsyncSearchActiveContext> asyncSearchContextOptional = asyncSearchActiveStore.getContext(asyncSearchContextId);
        if (asyncSearchContextOptional.isPresent()) {
            AsyncSearchActiveContext asyncSearchContext = asyncSearchContextOptional.get();
            asyncSearchStateMachine.trigger(new SearchFailureEvent(asyncSearchContext, exception));
            if (asyncSearchContext.shouldPersist()) {
                asyncSearchStateMachine.trigger(new BeginPersistEvent(asyncSearchContext));
            } else {
                //release active context from memory immediately as persistence is not required
                asyncSearchActiveStore.freeContext(asyncSearchContext.getContextId());
            }
            return asyncSearchContext.getAsyncSearchResponse();
        }
        return null;
    }

    public AsyncSearchResponse processSearchResponse(SearchResponse searchResponse, AsyncSearchContextId asyncSearchContextId) {
        final Optional<AsyncSearchActiveContext> asyncSearchContextOptional = asyncSearchActiveStore.getContext(asyncSearchContextId);
        if (asyncSearchContextOptional.isPresent()) {
            AsyncSearchActiveContext asyncSearchContext = asyncSearchContextOptional.get();
            asyncSearchStateMachine.trigger(new SearchSuccessfulEvent(asyncSearchContext, searchResponse));
            if (asyncSearchContext.shouldPersist()) {
                asyncSearchStateMachine.trigger(new BeginPersistEvent(asyncSearchContext));
            } else {
                //release active context from memory immediately as persistence is not required, in such cases a longer
                // wait_for_completion is expected
                asyncSearchActiveStore.freeContext(asyncSearchContext.getContextId());
            }
            return asyncSearchContext.getAsyncSearchResponse();
        }
        return null;
    }

    public void persistResponse(AsyncSearchActiveContext asyncSearchContext, AsyncSearchPersistenceModel persistenceModel) {
        //assert we are not post processing on any other thread pool
        assert Thread.currentThread().getName().contains(AsyncSearchPlugin.OPEN_DISTRO_ASYNC_SEARCH_GENERIC_THREAD_POOL_NAME);
        assert asyncSearchContext.retainedStages().contains(asyncSearchContext.getAsyncSearchState()) :
                "found stage " + asyncSearchContext.getAsyncSearchState() + "that shouldn't be retained";
        // acquire all permits non-blocking
        asyncSearchContext.acquireAllContextPermits(ActionListener.wrap(releasable -> {
                    // check again after acquiring permit if the context has been deleted mean while
                    if (asyncSearchContext.shouldPersist() == false) {
                        logger.warn(    "Async search context [{}] has been closed while waiting to acquire permits for post processing",
                                asyncSearchContext.getAsyncSearchId());
                        releasable.close();
                        return;
                    }
                    logger.warn("Persisting response for async search id [{}]", asyncSearchContext.getAsyncSearchId());
                    asyncSearchPersistenceService.storeResponse(asyncSearchContext.getAsyncSearchId(),
                            persistenceModel, ActionListener.runAfter(ActionListener.wrap(
                                    (indexResponse) -> {
                                        //Mark any dangling reference as PERSISTED and cleaning it up from the IN_MEMORY context
                                        logger.warn("Successfully persisted response for async search id [{}]",
                                                asyncSearchContext.getAsyncSearchId());
                                        asyncSearchStateMachine.trigger(new SearchResponsePersistedEvent(asyncSearchContext));
                                    },

                                    (e) -> {
                                        asyncSearchStateMachine.trigger(new SearchResponsePersistFailedEvent(asyncSearchContext));
                                        logger.error("Failed to persist final response for [{}] due to [{}]",
                                                asyncSearchContext.getAsyncSearchId(), e);
                                    }
                            ), releasable::close));

                }, (e) -> {
                    logger.error(() -> new ParameterizedMessage("Exception while acquiring the permit for asyncSearchContext [{}] due to ",
                            asyncSearchContext), e);
                    asyncSearchStateMachine.trigger(new SearchResponsePersistFailedEvent(asyncSearchContext));
                }),
                TimeValue.timeValueSeconds(60), "persisting response");
    }
}
