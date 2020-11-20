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

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.common.lease.Releasable;

/**
 * A listener guarantees that the {@linkplain Releasable} is released. Subclasses should implement notification logic with
 * innerOnResponse and innerOnFailure.
 */
public abstract class ReleasableActionListener implements ActionListener<Releasable> {

    protected abstract void innerOnResponse(Releasable response);

    protected abstract void innerOnFailure(Exception e);

    @Override
    public void onResponse(Releasable releasable) {
        try {
            innerOnResponse(releasable);
        } finally {
            releasable.close();
        }
    }

    @Override
    public void onFailure(Exception e) {
        innerOnFailure(e);
    }
}
