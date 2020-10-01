package com.amazon.opendistroforelasticsearch.search.async.listener;


import org.elasticsearch.action.ActionListener;

public interface PrioritizedActionListener<Response> extends ActionListener<Response> {

    void executeImmediately();
}
