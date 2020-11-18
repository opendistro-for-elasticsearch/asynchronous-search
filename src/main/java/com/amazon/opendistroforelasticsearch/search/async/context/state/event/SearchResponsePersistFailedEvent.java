package com.amazon.opendistroforelasticsearch.search.async.context.state.event;

import com.amazon.opendistroforelasticsearch.search.async.context.AsyncSearchContext;
import com.amazon.opendistroforelasticsearch.search.async.context.state.AsyncSearchContextEvent;

public class SearchResponsePersistFailedEvent extends AsyncSearchContextEvent {

    public SearchResponsePersistFailedEvent(AsyncSearchContext asyncSearchContext) {
        super(asyncSearchContext);
    }
}
