package com.amazon.opendistroforelasticsearch.search.async;

import com.amazon.opendistroforelasticsearch.search.async.listener.AsyncSearchContextListener;

import java.util.Set;

public interface TransitionProcessor<T> {

    /**
     * @return The valid transitions from the current {@link AsyncSearchStage}
     */
    Set<T> nextTransitions();

    /**
     *
     * @param contextListener Listeners to be invoked
     * @param asyncSearchContextId id of the context invoking listener
     */
    void onTransition(AsyncSearchContextListener contextListener, AsyncSearchContextId asyncSearchContextId);
}
