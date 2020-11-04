package com.amazon.opendistroforelasticsearch.search.async;

import com.amazon.opendistroforelasticsearch.search.async.listener.AsyncSearchContextListener;

import java.util.Set;

public interface TransitionProcessor<T> {

    /**
     * @return The valid transitions from the current {@link AsyncSearchStage}
     */
    Set<T> nextTransitions();

    /**
     * Listeners to be invoked
     */
    void onTransition(AsyncSearchContextListener contextListener, AsyncSearchContextId asyncSearchContextId);
}
