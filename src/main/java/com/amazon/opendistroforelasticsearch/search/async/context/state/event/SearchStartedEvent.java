package com.amazon.opendistroforelasticsearch.search.async.context.state.event;

import com.amazon.opendistroforelasticsearch.search.async.context.AsyncSearchContext;
import com.amazon.opendistroforelasticsearch.search.async.context.state.AsyncSearchContextEvent;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.search.SearchTask;

/**
 * Event triggered when
 * {@linkplain org.elasticsearch.action.search.TransportSearchAction#execute(ActionRequest, ActionListener)} is fired, to
 * signal the search has begun.
 */
public class SearchStartedEvent extends AsyncSearchContextEvent {

    private final SearchTask searchTask;

    public SearchStartedEvent(AsyncSearchContext asyncSearchContext, SearchTask searchTask) {
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
