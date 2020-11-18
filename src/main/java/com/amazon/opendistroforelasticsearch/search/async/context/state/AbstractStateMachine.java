package com.amazon.opendistroforelasticsearch.search.async.context.state;

import java.util.Set;

/**
 * {@linkplain AbstractStateMachine} provides APIs for generic finite state machine needed
 * for basic operations like working with states, events and a lifecycle.
 *
 * @param <State> the type of state
 * @param <Event> the type of event
 */
abstract class AbstractStateMachine<State, Event> {

    /**
     * Return FSM initial state.
     *
     * @return FSM initial state
     */
    abstract State getInitialState();

    /**
     * Return FSM final states.
     *
     * @return FSM final states
     */
    abstract Set<State> getFinalStates();

    /**
     * Return FSM registered states.
     *
     * @return FSM registered states
     */
    abstract Set<State> getStates();

    /**
     * Return FSM registered transitions.
     *
     * @return FSM registered transitions
     */
    abstract Set<? extends Transition<State, Event>> getTransitions();

    /**
     * Fire an event. According to event type, the FSM will make the right transition.
     *
     * @param event to fire
     * @return The next FSM state defined by the transition to make
     * @throws Exception thrown if an exception occurs during event handling
     */
    abstract <E extends Event> State trigger(E event) throws Exception;

}
