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

    /**
     *
     * @param contextId contextId
     */
    default void onContextFailed(AsyncSearchContextId contextId) {
    }

    /**
     *
     * @param asyncSearchContextId contextId
     */
    default void onContextPersisted(AsyncSearchContextId asyncSearchContextId) {
    }

    /**
     *
     * @param asyncSearchContextId contextId
     */
    default void onContextRunning(AsyncSearchContextId asyncSearchContextId) {

    }
}