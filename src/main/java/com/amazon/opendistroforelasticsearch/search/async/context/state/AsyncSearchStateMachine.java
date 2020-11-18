package com.amazon.opendistroforelasticsearch.search.async.context.state;


import com.amazon.opendistroforelasticsearch.search.async.context.AsyncSearchContextId;
import com.amazon.opendistroforelasticsearch.search.async.listener.AsyncSearchContextListener;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.HashSet;
import java.util.Set;
import java.util.function.BiConsumer;

public class AsyncSearchStateMachine extends AbstractStateMachine<AsyncSearchState, AsyncSearchContextEvent> {
    private AsyncSearchState initialState;
    private Set<AsyncSearchState> finalStates;
    private Set<AsyncSearchState> states;
    private Set<AsyncSearchTransition<? extends AsyncSearchContextEvent>> transitions;

    private static final Logger logger = LogManager.getLogger(AsyncSearchStateMachine.class);

    public AsyncSearchStateMachine(final Set<AsyncSearchState> states, final AsyncSearchState initialState) {
        this.states = states;
        this.initialState = initialState;
        transitions = new HashSet<>();
        finalStates = new HashSet<>();
    }

    public <Event extends AsyncSearchContextEvent> void registerTransition(final AsyncSearchTransition<Event> transition) {
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
    Set<? extends Transition<AsyncSearchState, AsyncSearchContextEvent>> getTransitions() {
        return transitions;
    }

    @Override
    <E extends AsyncSearchContextEvent> AsyncSearchState trigger(E event) {

        AsyncSearchState result;

        AsyncSearchState currentState = event.asyncSearchContext().getAsyncSearchStage();
        if (getFinalStates().contains(currentState)) {
            result = currentState;
        } else {
            for (Transition<AsyncSearchState, AsyncSearchContextEvent> transition : getTransitions()) {

                if (currentState.equals(transition.sourceState()) && transition.eventType().equals(event.getClass())) {

                    transition.onEvent().accept(currentState, event);
                    event.asyncSearchContext().setStage(transition.targetState());

                    BiConsumer<AsyncSearchContextId, AsyncSearchContextListener> eventListener =
                            ((AsyncSearchTransition<?>) transition).eventListener();
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

