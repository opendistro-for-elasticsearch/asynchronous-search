package com.amazon.opendistroforelasticsearch.search.async.context.state.event;

import com.amazon.opendistroforelasticsearch.search.async.context.AsyncSearchContext;
import com.amazon.opendistroforelasticsearch.search.async.context.state.AsyncSearchContextEvent;

/**
 * Event to trigger when an async search completes with an error.
 */
public class SearchFailureEvent extends AsyncSearchContextEvent {

    private final Exception exception;

    public SearchFailureEvent(AsyncSearchContext asyncSearchContext, Exception exception) {
        super(asyncSearchContext);
        this.exception = exception;
    }

    public Exception getException() {
        return exception;
    }
}
