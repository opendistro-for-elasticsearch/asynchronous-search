package com.amazon.opendistroforelasticsearch.search.async.context.state;

import org.elasticsearch.action.search.SearchTask;

/**
 * The state of the async search.
 */
public enum AsyncSearchState {

    /**
     * At the start of the search, before the {@link SearchTask} starts to run
     */
    INIT,

    /**
     * The search state actually has been started
     */
    RUNNING,

    /**
     * The search has completed successfully
     */
    SUCCEEDED,

    /**
     * The search execution has failed
     */
    FAILED,

    /**
     * The context has been persisted to system index
     */
    PERSISTED,

    /**
     * The context has failed to persist to system index
     */
    PERSIST_FAILED,

    /**
     * The context has been deleted. Terminal stage.
     */
    DELETED
}
