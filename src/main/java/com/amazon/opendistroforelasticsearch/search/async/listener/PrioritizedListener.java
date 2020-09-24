package com.amazon.opendistroforelasticsearch.search.async.listener;


import org.elasticsearch.action.ActionListener;

public interface PrioritizedListener<Response> extends ActionListener<Response>, Runnable {

    void executeImmediately();
}
