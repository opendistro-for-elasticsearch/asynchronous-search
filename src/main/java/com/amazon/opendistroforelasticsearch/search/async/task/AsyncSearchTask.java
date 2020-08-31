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

package com.amazon.opendistroforelasticsearch.search.async.task;

import com.amazon.opendistroforelasticsearch.search.async.AsyncSearchContext;
import com.amazon.opendistroforelasticsearch.search.async.AsyncSearchId;
import com.amazon.opendistroforelasticsearch.search.async.listener.AsyncSearchProgressActionListener;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.search.SearchTask;
import org.elasticsearch.common.lease.Releasable;
import org.elasticsearch.tasks.TaskId;

import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

public class AsyncSearchTask extends SearchTask {

    Logger logger = LogManager.getLogger(AsyncSearchTask.class);

    private AsyncSearchProgressActionListener progressActionListener;
    private List<Releasable> onCancelledReleasables;

    public AsyncSearchTask(long id, String type, String action, String description, TaskId parentTaskId, Map<String, String> headers) {
        super(id, type, action, description, parentTaskId, headers);
        onCancelledReleasables = new LinkedList<>();
    }

    /**
     * Attach a {@link AsyncSearchProgressActionListener} to this task.
     */
    public final void setProgressListener(AsyncSearchProgressActionListener progressActionListener) {
        this.progressActionListener = progressActionListener;
        super.setProgressListener(progressActionListener);
    }

    @Override
    protected void onCancelled() {
        onCancelledReleasables.forEach(releasable -> {
            try{
                releasable.close();
            } catch (Exception e) {
                logger.error("Failed to close releasable", e);
            }
        });
    }

    public void addOncancelledReleasables(List<Releasable> onCancelledReleasables) {
        this.onCancelledReleasables.addAll(onCancelledReleasables);
    }
}

