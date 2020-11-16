package com.amazon.opendistroforelasticsearch.search.async.context.state;

import com.amazon.opendistroforelasticsearch.search.async.context.AsyncSearchContext;

import java.util.Objects;

/**
 * The AsyncSearchContextEvent on which the transitions take place
 */
public abstract class AsyncSearchContextEvent {

    protected final AsyncSearchContext asyncSearchContext;

    protected AsyncSearchContextEvent(AsyncSearchContext asyncSearchContext) {
        Objects.requireNonNull(asyncSearchContext);
        this.asyncSearchContext = asyncSearchContext;
    }

    /**
     * Name of the event.
     * @return event name
     */
    public abstract String eventName();

    public AsyncSearchContext asyncSearchContext() {
        return asyncSearchContext;
    }

}

