package com.amazon.opendistroforelasticsearch.search.async.context.state;

import com.amazon.opendistroforelasticsearch.search.async.context.AsyncSearchContextId;
import com.amazon.opendistroforelasticsearch.search.async.listener.AsyncSearchContextListener;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;

import java.util.Set;
import java.util.HashSet;
import java.util.function.BiConsumer;

public abstract class AbstractStateMachine implements StateMachine<AsyncSearchState, AsyncSearchContextEvent> {

    protected Set<AsyncSearchTransition<? extends AsyncSearchContextEvent>> transitions;

    private static final Logger logger = LogManager.getLogger(AbstractStateMachine.class);

    AbstractStateMachine() {
        transitions = new HashSet<>();
    }

    @Override
    public Set<AsyncSearchTransition<? extends AsyncSearchContextEvent>> getTransitions() {
        return transitions;
    }

    public void registerTransition(AsyncSearchTransition<? extends AsyncSearchContextEvent> transition) {
        transitions.add(transition);
    }

    @Override
    public AsyncSearchState trigger(AsyncSearchContextEvent event) {
        AsyncSearchState result;
        AsyncSearchState currentState = event.asyncSearchContext().getAsyncSearchStage();
        if (getFinalStates().contains(currentState)) {
            result = currentState;
        } else {
            for (AsyncSearchTransition<? extends AsyncSearchContextEvent> transition : getTransitions()) {
                if (currentState.equals(transition.sourceState()) && transition.eventType().equals(event.getClass())) {
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
                }
            }
            result = event.asyncSearchContext().getAsyncSearchStage();
        }
        return result;
    }

    @SuppressWarnings("unchecked")
    //Suppress the warning since we know the type of the event and transition based on the validation
    private <T> void execute(BiConsumer<AsyncSearchState, T> onEvent, AsyncSearchContextEvent event, AsyncSearchState state) {
        onEvent.accept(state, (T)event);
    }
}
