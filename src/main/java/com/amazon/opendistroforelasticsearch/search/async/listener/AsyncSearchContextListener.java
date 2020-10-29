/*
 *   Copyright 2020 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 *   Licensed under the Apache License, Version 2.0 (the "License").
 *   You may not use this file except in compliance with the License.
 *   A copy of the License is located at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *   or in the "license" file accompanying this file. This file is distributed
 *   on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 *   express or implied. See the License for the specific language governing
 *   permissions and limitations under the License.
 */

package com.amazon.opendistroforelasticsearch.search.async.listener;

import com.amazon.opendistroforelasticsearch.search.async.AsyncSearchContextId;

/**
 * An listener for async search context events.
 */
public interface AsyncSearchContextListener {

    /**
     * Executed when a new async search context was created
     */
    default void onNewContext(AsyncSearchContextId contextId) {
    }

    /**
     * Executed when a previously created async search context completes.
     */
    default void onContextCompleted(AsyncSearchContextId contextId) {
    }

    /**
     *  Executed when a previously created async search context fails.
     */
    default void onContextFailed(AsyncSearchContextId contextId) {
    }

    /**
     * Executed when a previously created async search context is persisted.
     */
    default void onContextPersisted(AsyncSearchContextId contextId) {
    }

    /**
     * Executed when a previously created async search context fails persisting.
     */
    default void onContextPersistFailed(AsyncSearchContextId contextId) {
    }

    /**
     * Executed when a previously created async search context is deleted.
     */
    default void onContextDeleted(AsyncSearchContextId contextId) {
    }

    /**
     * Executed when a previously created async search context is running.
     */
    default void onContextRunning(AsyncSearchContextId contextId) {

    }
}