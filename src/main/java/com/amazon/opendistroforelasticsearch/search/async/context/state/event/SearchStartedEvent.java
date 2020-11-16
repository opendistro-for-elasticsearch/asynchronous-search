package com.amazon.opendistroforelasticsearch.search.async.context.state.event;

import com.amazon.opendistroforelasticsearch.search.async.context.AsyncSearchContext;
import com.amazon.opendistroforelasticsearch.search.async.context.state.AsyncSearchContextEvent;

public class SearchStartedEvent extends AsyncSearchContextEvent {

    SearchStartedEvent(AsyncSearchContext asyncSearchContext, Exception exception) {
        super(asyncSearchContext);
    }

    @Override
    public String eventName() {
        return "search_started";
    }

    @Override
    public AsyncSearchContext asyncSearchContext() {
        return asyncSearchContext;
    }
}
