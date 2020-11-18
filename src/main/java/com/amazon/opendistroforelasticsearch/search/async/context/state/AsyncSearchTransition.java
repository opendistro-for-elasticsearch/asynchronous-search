package com.amazon.opendistroforelasticsearch.search.async.context.state;

import com.amazon.opendistroforelasticsearch.search.async.context.AsyncSearchContextId;
import com.amazon.opendistroforelasticsearch.search.async.listener.AsyncSearchContextListener;

import java.util.function.BiConsumer;

public class AsyncSearchTransition<Event extends AsyncSearchContextEvent> implements Transition<AsyncSearchState,
        AsyncSearchContextEvent> {

    private final AsyncSearchState sourceState;
    private final AsyncSearchState targetState;
    private final BiConsumer<AsyncSearchState, Event> onEvent;
    private final BiConsumer<AsyncSearchContextId, AsyncSearchContextListener> eventListener;
    private final Class<Event> eventType;

    public AsyncSearchTransition(AsyncSearchState sourceState, AsyncSearchState targetState,
                                 BiConsumer<AsyncSearchState, Event> onEvent,
                                 BiConsumer<AsyncSearchContextId, AsyncSearchContextListener> eventListener,
                                 Class<Event> eventName) {
        this.sourceState = sourceState;
        this.targetState = targetState;
        this.onEvent = onEvent;
        this.eventListener = eventListener;
        this.eventType = eventName;
    }

    @Override
    public AsyncSearchState sourceState() {
        return sourceState;
    }

    @Override
    public AsyncSearchState targetState() {
        return targetState;
    }

    @Override
    public Class<? extends AsyncSearchContextEvent> eventType() {
        return eventType;
    }

    @Override
    public BiConsumer<AsyncSearchState, Event> onEvent() {
        return onEvent;
    }

    @Override
    public BiConsumer<AsyncSearchContextId, AsyncSearchContextListener> eventListener() {
        return eventListener;
    }
}
