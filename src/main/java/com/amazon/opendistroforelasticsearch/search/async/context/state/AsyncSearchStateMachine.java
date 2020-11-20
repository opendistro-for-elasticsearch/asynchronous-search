package com.amazon.opendistroforelasticsearch.search.async.context.state;

import com.amazon.opendistroforelasticsearch.search.async.context.AsyncSearchContext;
import com.amazon.opendistroforelasticsearch.search.async.context.AsyncSearchContextId;
import com.amazon.opendistroforelasticsearch.search.async.context.state.exception.AsyncSearchStateMachineException;
import com.amazon.opendistroforelasticsearch.search.async.listener.AsyncSearchContextListener;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;

import java.util.HashSet;
import java.util.Set;
import java.util.function.BiConsumer;

/**
 * The FSM encapsulating the lifecycle of an async search request. It contains the  list of valid Async search states and
 * the valid transitions that an {@linkplain AsyncSearchContext} can make.
 */
public class AsyncSearchStateMachine implements StateMachine<AsyncSearchState, AsyncSearchContextEvent> {

    private static final Logger logger = LogManager.getLogger(AsyncSearchStateMachine.class);

    private final Set<AsyncSearchTransition<? extends AsyncSearchContextEvent>> transitions;
    private final AsyncSearchState initialState;
    private Set<AsyncSearchState> finalStates;
    private Set<AsyncSearchState> states;

    public AsyncSearchStateMachine(final Set<AsyncSearchState> states, final AsyncSearchState initialState) {
        super();
        this.transitions = new HashSet<>();
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

    @Override
    public Set<AsyncSearchTransition<? extends AsyncSearchContextEvent>> getTransitions() {
        return transitions;
    }

    public void registerTransition(AsyncSearchTransition<? extends AsyncSearchContextEvent> transition) {
        transitions.add(transition);
    }


    /**
     * Triggers transition from current state on receiving an event. Also invokes {@linkplain Transition#onEvent()} and
     * {@linkplain Transition#eventListener()}
     *
     * @param event to fire
     * @return The final Async search state
     * @throws AsyncSearchStateMachineException when no transition is found  from current state on given event
     */
    @Override
    public AsyncSearchState trigger(AsyncSearchContextEvent event) throws AsyncSearchStateMachineException {
        synchronized (event.asyncSearchContext()) {
            AsyncSearchState currentState = event.asyncSearchContext().getAsyncSearchStage();
            for (AsyncSearchTransition<? extends AsyncSearchContextEvent> transition : getTransitions()) {
                if (currentState.equals(transition.sourceState()) && transition.eventType().isInstance(event)) {
                    execute(transition.onEvent(), event, currentState);
                    event.asyncSearchContext().setState(transition.targetState());
                    logger.debug("Executed event {} for async event {} ", event.getClass().getName(),
                            event.asyncSearchContext.getAsyncSearchId());
                    BiConsumer<AsyncSearchContextId, AsyncSearchContextListener> eventListener = transition.eventListener();
                    try {
                        eventListener.accept(event.asyncSearchContext().getContextId(), event.asyncSearchContext().getContextListener());
                    } catch (Exception ex) {
                        logger.error(() -> new ParameterizedMessage("Failed to execute listener for async search id : {}",
                                event.asyncSearchContext.getAsyncSearchId()), ex);
                    }
                    return event.asyncSearchContext().getAsyncSearchStage();
                }
            }
            throw new AsyncSearchStateMachineException(currentState, event);
        }

    }

    @SuppressWarnings("unchecked")
    //Suppress the warning since we know the type of the event and transition based on the validation
    private <T> void execute(BiConsumer<AsyncSearchState, T> onEvent, AsyncSearchContextEvent event, AsyncSearchState state) {
        onEvent.accept(state, (T) event);
    }

}

