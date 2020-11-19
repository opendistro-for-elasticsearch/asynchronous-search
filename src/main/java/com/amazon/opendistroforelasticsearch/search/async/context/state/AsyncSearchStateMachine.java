package com.amazon.opendistroforelasticsearch.search.async.context.state;


import com.amazon.opendistroforelasticsearch.search.async.context.AsyncSearchContext;
import com.amazon.opendistroforelasticsearch.search.async.context.AsyncSearchContextId;
import com.amazon.opendistroforelasticsearch.search.async.listener.AsyncSearchContextListener;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.HashSet;
import java.util.Set;
import java.util.function.BiConsumer;

/**
 * The FSM encapsulating the lifecycle of an async search request. It contains the  list of valid Async search states and
 * the valid transitions that an {@linkplain AsyncSearchContext} can make.
 */
public class AsyncSearchStateMachine extends AbstractStateMachine<AsyncSearchState, AsyncSearchContextEvent> {
    private AsyncSearchState initialState;
    private Set<AsyncSearchState> finalStates;
    private Set<AsyncSearchState> states;
    private Set<AsyncSearchTransition> transitions;

    private static final Logger logger = LogManager.getLogger(AsyncSearchStateMachine.class);

    public AsyncSearchStateMachine(final Set<AsyncSearchState> states, final AsyncSearchState initialState) {
        this.states = states;
        this.initialState = initialState;
        transitions = new HashSet<>();
        finalStates = new HashSet<>();
    }

    public void registerTransition(final AsyncSearchTransition transition) {
        transitions.add(transition);
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

    @Override
    Set<AsyncSearchTransition> getTransitions() {
        return transitions;
    }

    @Override
    public <E extends AsyncSearchContextEvent> AsyncSearchState trigger(E event) {

        AsyncSearchState result;

        AsyncSearchState currentState = event.asyncSearchContext().getAsyncSearchStage();
        if (getFinalStates().contains(currentState)) {
            result = currentState;
        } else {
            for (AsyncSearchTransition transition : getTransitions()) {
                if (currentState.equals(transition.sourceState()) && transition.eventType().isInstance(event)) {

                    transition.onEvent().accept(currentState, event);
                    event.asyncSearchContext().setStage(transition.targetState());

                    BiConsumer<AsyncSearchContextId, AsyncSearchContextListener> eventListener =
                            transition.eventListener();
                    eventListener.accept(event.asyncSearchContext().getContextId(),
                            event.asyncSearchContext().getContextListener());

                    logger.debug("Executed event {} for async event {} ",
                            event, event.asyncSearchContext);

                }
            }
            result = event.asyncSearchContext().getAsyncSearchStage();
        }
        return result;
    }
}

