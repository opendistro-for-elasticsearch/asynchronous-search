package com.amazon.opendistroforelasticsearch.search.async.context.state.event;

import com.amazon.opendistroforelasticsearch.search.async.context.AsyncSearchContext;
import com.amazon.opendistroforelasticsearch.search.async.context.state.AsyncSearchContextEvent;

public class SearchFailureEvent extends AsyncSearchContextEvent {

    private Exception exception;

    public SearchFailureEvent(AsyncSearchContext asyncSearchContext, Exception exception) {
        super(asyncSearchContext);
        this.exception = exception;
    }

    @Override
    public String eventName() {
        return "search_failed";
    }

    public Exception getException() {
        return exception;
    }
}
