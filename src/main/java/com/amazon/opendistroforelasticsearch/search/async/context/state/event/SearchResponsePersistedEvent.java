package com.amazon.opendistroforelasticsearch.search.async.context.state.event;

import com.amazon.opendistroforelasticsearch.search.async.context.AsyncSearchContext;
import com.amazon.opendistroforelasticsearch.search.async.context.state.AsyncSearchContextEvent;

public class SearchResponsePersistedEvent extends AsyncSearchContextEvent {

    public SearchResponsePersistedEvent(AsyncSearchContext asyncSearchContext) {
        super(asyncSearchContext);
    }
}

