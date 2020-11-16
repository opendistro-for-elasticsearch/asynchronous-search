package com.amazon.opendistroforelasticsearch.search.async.context.state;


import com.amazon.opendistroforelasticsearch.search.async.context.AsyncSearchContextId;
import com.amazon.opendistroforelasticsearch.search.async.listener.AsyncSearchContextListener;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;

import java.util.HashSet;
import java.util.Set;
import java.util.function.BiConsumer;

public class AsyncSearchStateMachine extends AbstractStateMachine<AsyncSearchState, AsyncSearchContextEvent> {
    private AsyncSearchState initialState;
    private Set<AsyncSearchState> finalStates;
    private Set<AsyncSearchState> states;
    private Set<AsyncSearchTransition<AsyncSearchContextEvent>> transitions;

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
    public Set<AsyncSearchTransition<AsyncSearchContextEvent>> getTransitions() {
        return transitions;
    }

    @Override
    AsyncSearchState trigger(AsyncSearchContextEvent event) {
        AsyncSearchState currentState = event.asyncSearchContext().getAsyncSearchStage();
        if (getFinalStates().isEmpty() == false && getFinalStates().contains(currentState)) {
            return currentState;
        }

        if (event == null) {
            return currentState;
        }

        for (Transition<AsyncSearchState, AsyncSearchContextEvent> transition : getTransitions()) {
            if (currentState.equals(transition.sourceState()) && getStates().contains(transition.targetState())) {
                //perform action, if any
                if (transition.onEvent() != null) {
                    transition.onEvent().accept(currentState, event);
                    BiConsumer<AsyncSearchContextId, AsyncSearchContextListener> eventListener =
                            (BiConsumer<AsyncSearchContextId, AsyncSearchContextListener>) transition.eventListener();
                    transition.move().accept(event, transition.targetState());
                    //event.asyncSearchContext().setStage(transition.targetState());
                    try {
                        eventListener.accept(event.asyncSearchContext().getContextId(), event.asyncSearchContext().getContextListener());
                        logger.debug("Executed event {} for async event {} ", event, event.asyncSearchContext);
                    } catch (Exception e) {
                        logger.error(() -> new ParameterizedMessage("Failed to execute listener for event {} ", event), e);
                    }
                }
            }
        }
        return currentState;
    }
}

