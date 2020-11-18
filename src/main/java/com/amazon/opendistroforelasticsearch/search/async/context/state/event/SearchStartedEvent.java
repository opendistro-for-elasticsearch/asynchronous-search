package com.amazon.opendistroforelasticsearch.search.async.context.state.event;

import com.amazon.opendistroforelasticsearch.search.async.context.AsyncSearchContext;
import com.amazon.opendistroforelasticsearch.search.async.context.state.AsyncSearchContextEvent;
import org.elasticsearch.action.search.SearchTask;

public class SearchStartedEvent extends AsyncSearchContextEvent {

    private final SearchTask searchTask;

    SearchStartedEvent(AsyncSearchContext asyncSearchContext, SearchTask searchTask) {
        super(asyncSearchContext);
        this.searchTask = searchTask;
    }

    @Override
    public AsyncSearchContext asyncSearchContext() {
        return asyncSearchContext;
    }

    public SearchTask getSearchTask() {
        return searchTask;
    }
}
