package com.amazon.opendistroforelasticsearch.search.async.context.state.event;

import com.amazon.opendistroforelasticsearch.search.async.context.AsyncSearchContext;
import com.amazon.opendistroforelasticsearch.search.async.context.state.AsyncSearchContextEvent;

public class SearchDeletionEvent extends AsyncSearchContextEvent {

    public SearchDeletionEvent(AsyncSearchContext asyncSearchContext) {
        super(asyncSearchContext);
    }
}
