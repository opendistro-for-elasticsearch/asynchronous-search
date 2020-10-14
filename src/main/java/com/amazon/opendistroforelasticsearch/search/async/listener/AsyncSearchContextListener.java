package com.amazon.opendistroforelasticsearch.search.async.listener;

import com.amazon.opendistroforelasticsearch.search.async.AsyncSearchContextId;

/**
 * An listener for async search context events.
 */
public interface AsyncSearchContextListener {

    /**
     * Executed when a new async search context was created
     *
     * @param context the created context
     */
    default void onNewContext(AsyncSearchContextId context) {
    }

    /**
     * Executed when a previously created async search context is freed.
     * This happens either when the async search execution finishes, if the
     * execution failed or if the search context as idle for and needs to be
     * cleaned up.
     *
     * @param context the freed search context
     */
    default void onFreeContext(AsyncSearchContextId context) {
    }

    /**
     * Executed when a previously created async search context is running.
     *
     * @param context the freed search context
     */
    default void onContextCompleted(AsyncSearchContextId context) {
    }

    /**
     * Executed when a previously created async search context is cancelled.
     *
     * @param context the freed search context
     */
    default void onContextCancelled(AsyncSearchContextId context) {
    }

    default void onContextFailed(AsyncSearchContextId contextId) {
    }

    default void onContextPersisted(AsyncSearchContextId asyncSearchContextId) {
    }
}