package com.amazon.opendistroforelasticsearch.search.async.listener;

import com.amazon.opendistroforelasticsearch.search.async.AsyncSearchContext;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;

import java.util.List;

/**
 * An listener for async search context events.
 */
public interface AsyncSearchContextListener {

    /**
     * Executed when a new async search context was created
     * @param context the created context
     */
    default void onNewContext(AsyncSearchContext context) {}

    /**
     * Executed when a previously created async search context is freed.
     * This happens either when the async search execution finishes, if the
     * execution failed or if the search context as idle for and needs to be
     * cleaned up.
     * @param context the freed search context
     */
    default void onFreeContext(AsyncSearchContext context) {}

    /**
     * Executed when a previously created async search context is running.
     * @param context the freed search context
     */
    default void onContextRunning(AsyncSearchContext context) {}

    /**
     * Executed when a previously created async search context is cancelled.
     * @param context the freed search context
     */
    default void onContextCancelled(AsyncSearchContext context) {}



    /**
     * A Composite listener that multiplexes calls to each of the listeners methods.
     */
    final class CompositeListener implements AsyncSearchContextListener {
        private final List<AsyncSearchContextListener> listeners;
        private final Logger logger;

        public CompositeListener(List<AsyncSearchContextListener> listeners, Logger logger) {
            this.listeners = listeners;
            this.logger = logger;
        }

        public void addListener(AsyncSearchContextListener listener) {
            this.listeners.add(listener);
        }


        @Override
        public void onNewContext(AsyncSearchContext context) {
            for (AsyncSearchContextListener listener : listeners) {
                try {
                    listener.onNewContext(context);
                } catch (Exception e) {
                    logger.warn(() -> new ParameterizedMessage("onNewContext listener [{}] failed", listener), e);
                }
            }
        }

        @Override
        public void onFreeContext(AsyncSearchContext context) {
            for (AsyncSearchContextListener listener : listeners) {
                try {
                    listener.onFreeContext(context);
                } catch (Exception e) {
                    logger.warn(() -> new ParameterizedMessage("onFreeContext listener [{}] failed", listener), e);
                }
            }
        }

        @Override
        public void onContextRunning(AsyncSearchContext context) {
            for (AsyncSearchContextListener listener : listeners) {
                try {
                    listener.onContextRunning(context);
                } catch (Exception e) {
                    logger.warn(() -> new ParameterizedMessage("onFreeContext listener [{}] failed", listener), e);
                }
            }
        }

        @Override
        public void onContextCancelled(AsyncSearchContext context) {
            for (AsyncSearchContextListener listener : listeners) {
                try {
                    listener.onContextCancelled(context);
                } catch (Exception e) {
                    logger.warn(() -> new ParameterizedMessage("onFreeContext listener [{}] failed", listener), e);
                }
            }
        }
    }
}