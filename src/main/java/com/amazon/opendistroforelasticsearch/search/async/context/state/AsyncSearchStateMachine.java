package com.amazon.opendistroforelasticsearch.search.async.context.state;

import java.util.HashSet;
import java.util.Set;

public class AsyncSearchStateMachine extends AbstractStateMachine {

    private AsyncSearchState initialState;
    private Set<AsyncSearchState> finalStates;
    private Set<AsyncSearchState> states;

    public AsyncSearchStateMachine(final Set<AsyncSearchState> states, final AsyncSearchState initialState) {
        super();
        this.states = states;
        this.initialState = initialState;
        this.finalStates = new HashSet<>();
    }

    public void markTerminalStates(final Set<AsyncSearchState> finalStates) {
        this.finalStates = finalStates;
    }

    @Override
    public AsyncSearchState getInitialState() {
        return initialState;
    }

    @Override
    public Set<AsyncSearchState> getFinalStates() {
        return finalStates;
    }

    @Override
    public Set<AsyncSearchState> getStates() {
        return states;
    }
}

