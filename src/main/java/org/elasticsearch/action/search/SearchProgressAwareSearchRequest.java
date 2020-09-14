package org.elasticsearch.action.search;

import org.elasticsearch.tasks.TaskId;

import java.util.Map;

public class SearchProgressAwareSearchRequest extends SearchRequest {

    private SearchProgressActionListener searchProgressActionListener;

    public SearchProgressActionListener getSearchProgressActionListener() {
        return searchProgressActionListener;
    }

    public void setSearchProgressActionListener(SearchProgressActionListener searchProgressActionListener) {
        this.searchProgressActionListener = searchProgressActionListener;
    }

    @Override
    public SearchTask createTask(long id, String type, String action, TaskId parentTaskId, Map<String, String> headers) {
        SearchTask task = super.createTask(id, type, action, parentTaskId, headers);
        task.setProgressListener(searchProgressActionListener);
        return task;
    }
}
