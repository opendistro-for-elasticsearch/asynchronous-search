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
import com.amazon.opendistroforelasticsearch.search.async.listener.AsyncSearchProgressActionListener;
import org.elasticsearch.action.search.SearchTask;
import org.elasticsearch.tasks.TaskId;

import java.util.Map;

public class AsyncSearchTask extends SearchTask {

    private AsyncSearchProgressActionListener progressActionListener;
    private AsyncSearchContext asyncSearchContext;

    public void setAsyncSearchContext(AsyncSearchContext asyncSearchContext) {
        this.asyncSearchContext = asyncSearchContext;
    }

    public AsyncSearchTask(long id, String type, String action, String description, TaskId parentTaskId, Map<String, String> headers) {
        super(id, type, action, description, parentTaskId, headers);
    }

    /**
     * Attach a {@link AsyncSearchProgressActionListener} to this task.
     */
    public final void setProgressListener(AsyncSearchProgressActionListener progressActionListener) {
        this.progressActionListener = progressActionListener;
        super.setProgressListener(progressActionListener);
    }
    public final AsyncSearchProgressActionListener getProgressActionListener() {
        return progressActionListener;
    }

    @Override
    protected void onCancelled() {
        if(asyncSearchContext != null)
        asyncSearchContext.clear();
    }
}

