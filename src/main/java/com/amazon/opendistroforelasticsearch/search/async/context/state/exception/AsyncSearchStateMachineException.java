package com.amazon.opendistroforelasticsearch.search.async.context.state.exception;

import com.amazon.opendistroforelasticsearch.search.async.context.state.AsyncSearchContextEvent;
import com.amazon.opendistroforelasticsearch.search.async.context.state.AsyncSearchState;

public class AsyncSearchStateMachineException extends IllegalStateException {
    private final AsyncSearchState currentState;
    private final AsyncSearchContextEvent event;

    public AsyncSearchStateMachineException(AsyncSearchState currentState, AsyncSearchContextEvent event) {
        super(event.asyncSearchContext().getAsyncSearchId()
                + " cannot transition from [" + currentState + "] on event " + event.getClass());
        this.event = event;
        this.currentState = currentState;
    }
}
