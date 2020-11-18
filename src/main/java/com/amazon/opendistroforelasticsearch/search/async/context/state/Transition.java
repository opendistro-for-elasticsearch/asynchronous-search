package com.amazon.opendistroforelasticsearch.search.async.context.state;

import java.util.function.BiConsumer;

public interface Transition<State, Event> {

    /**
     * Return transition source state.
     *
     * @return transition source state
     */
    State sourceState();

    /**
     * Return transition target state.
     *
     * @return transition target state
     */
    State targetState();

    Class<? extends Event> eventType();

    /**
     * Return a consumer to execute when an event is fired.
     *
     * @return a bi-consumer
     */
    BiConsumer<State, ? extends Event> onEvent();

    /**
     * Event listener to be invoked on transition
     *
     * @return the event listener
     */

    BiConsumer<?, ?> eventListener();

}
