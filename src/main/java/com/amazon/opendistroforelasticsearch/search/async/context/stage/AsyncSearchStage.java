package com.amazon.opendistroforelasticsearch.search.async.context.stage;

import com.amazon.opendistroforelasticsearch.search.async.context.AsyncSearchContextId;
import com.amazon.opendistroforelasticsearch.search.async.listener.AsyncSearchContextListener;
import org.elasticsearch.action.search.SearchTask;
import org.elasticsearch.common.util.set.Sets;

import java.util.Collections;
import java.util.Set;

/**
 * The state of the async search.
 */
public enum AsyncSearchStage implements TransitionProcessor<AsyncSearchStage> {
    /**
     * At the start of the search, before the {@link SearchTask} starts to run
     */
    INIT {
        @Override
        public Set<AsyncSearchStage> nextTransitions() {
            return Sets.newHashSet(RUNNING);
        }

        /**
         * @param contextListener Listener to be notified on transition
         * @param asyncSearchContextId context which has undergone the transition
         */
        @Override
        public void onTransition(AsyncSearchContextListener contextListener,
                                 AsyncSearchContextId asyncSearchContextId) {
            contextListener.onNewContext(asyncSearchContextId);
        }
    },
    /**
     * The search state actually has been started
     */
    RUNNING {
        @Override
        public Set<AsyncSearchStage> nextTransitions() {
            return Sets.newHashSet(SUCCEEDED, FAILED, DELETED);
        }

        /**
         * @param contextListener Listener to be notified on transition
         * @param asyncSearchContextId context which has undergone the transition
         */

        @Override
        public void onTransition(AsyncSearchContextListener contextListener,
                                 AsyncSearchContextId asyncSearchContextId) {
            contextListener.onContextRunning(asyncSearchContextId);
        }
    },
    /**
     * The search has completed successfully
     */
    SUCCEEDED {
        @Override
        public Set<AsyncSearchStage> nextTransitions() {
            return Sets.newHashSet(PERSISTED, PERSIST_FAILED, DELETED);
        }

        /**
         * @param contextListener Listener to be notified on transition
         * @param asyncSearchContextId context which has undergone the transition
         */

        @Override
        public void onTransition(AsyncSearchContextListener contextListener,
                                 AsyncSearchContextId asyncSearchContextId) {
            contextListener.onContextCompleted(asyncSearchContextId);
        }
    },
    /**
     * The search execution has failed
     */
    FAILED {
        @Override
        public Set<AsyncSearchStage> nextTransitions() {
            return Sets.newHashSet(PERSISTED, PERSIST_FAILED, DELETED);
        }

        /**
         * @param contextListener Listener to be notified on transition
         * @param asyncSearchContextId context which has undergone the transition
         */

        @Override
        public void onTransition(AsyncSearchContextListener contextListener,
                                 AsyncSearchContextId asyncSearchContextId) {
            contextListener.onContextFailed(asyncSearchContextId);
        }
    },
    /**
     * The context has been persisted to system index
     */
    PERSISTED {
        @Override
        public Set<AsyncSearchStage> nextTransitions() {
            return Sets.newHashSet(DELETED);
        }

        /**
         * @param contextListener Listener to be notified on transition
         * @param asyncSearchContextId context which has undergone the transition
         */

        @Override
        public void onTransition(AsyncSearchContextListener contextListener,
                                 AsyncSearchContextId asyncSearchContextId) {
            contextListener.onContextPersisted(asyncSearchContextId);
        }
    },
    /**
     * The context has failed to persist to system index
     */
    PERSIST_FAILED {
        @Override
        public Set<AsyncSearchStage> nextTransitions() {
            return Sets.newHashSet(DELETED);
        }

        /**
         * @param contextListener Listener to be notified on transition
         * @param asyncSearchContextId context which has undergone the transition
         */

        @Override
        public void onTransition(AsyncSearchContextListener contextListener,
                                 AsyncSearchContextId asyncSearchContextId) {
            contextListener.onContextPersistFailed(asyncSearchContextId);
        }
    },
    /**
     * The context has been deleted. Terminal stage.
     */
    DELETED {
        @Override
        public Set<AsyncSearchStage> nextTransitions() {
            return Collections.emptySet();
        }

        /**
         * @param contextListener Listener to be notified on transition
         * @param asyncSearchContextId context which has undergone the transition
         */

        @Override
        public void onTransition(AsyncSearchContextListener contextListener,
                                 AsyncSearchContextId asyncSearchContextId) {
            contextListener.onContextDeleted(asyncSearchContextId);
        }
    };
}
