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
     *
     * @param context the created context
     */
    default void onNewContext(AsyncSearchContextId context) {
    }

    /**
     * Executed when a previously created async search context is running.
     *
     * @param context the freed search context
     */
    default void onContextCompleted(AsyncSearchContextId context) {
    }

    /**
     *
     * @param contextId contextId
     */
    default void onContextFailed(AsyncSearchContextId contextId) {
    }

    /**
     *
     * @param asyncSearchContextId contextId
     */
    default void onContextPersisted(AsyncSearchContextId asyncSearchContextId) {
    }

    /**
     *
     * @param asyncSearchContextId contextId
     */
    default void onContextRunning(AsyncSearchContextId asyncSearchContextId) {

    }
}