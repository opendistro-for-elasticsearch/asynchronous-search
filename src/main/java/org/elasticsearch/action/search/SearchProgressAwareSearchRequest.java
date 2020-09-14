package org.elasticsearch.action.search;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.search.builder.SearchSourceBuilder;

import java.io.IOException;

public class CustomSearchRequest extends SearchRequest {
    public CustomSearchRequest() {
    }

    public CustomSearchRequest(SearchRequest searchRequest) {
        super(searchRequest);
    }

    public CustomSearchRequest(String... indices) {
        super(indices);
    }

    public CustomSearchRequest(String[] indices, SearchSourceBuilder source) {
        super(indices, source);
    }

    public CustomSearchRequest(StreamInput in) throws IOException {
        super(in);
    }

    public SearchProgressActionListener getSearchProgressActionListener() {
        return searchProgressActionListener;
    }

    public void setSearchProgressActionListener(SearchProgressActionListener searchProgressActionListener) {
        this.searchProgressActionListener = searchProgressActionListener;
    }

    private SearchProgressActionListener searchProgressActionListener;
}
