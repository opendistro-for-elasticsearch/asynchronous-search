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

package com.amazon.opendistroforelasticsearch.search.async.persistence;

import com.amazon.opendistroforelasticsearch.search.async.AsyncSearchContext;
import com.amazon.opendistroforelasticsearch.search.async.AsyncSearchId;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.xcontent.LoggingDeprecationHandler;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;

import java.io.IOException;
import java.util.Objects;

/**
 * Represents a persisted version of {@link AsyncSearchContext} through a backing {@link AsyncSearchPersistenceModel}
 */
public class AsyncSearchPersistenceContext extends AsyncSearchContext {

    private static final Logger logger = LogManager.getLogger(AsyncSearchPersistenceContext.class);

    private final AsyncSearchId asyncSearchId;
    private final AsyncSearchPersistenceModel asyncSearchPersistenceModel;

    public AsyncSearchPersistenceContext(AsyncSearchId asyncSearchId, AsyncSearchPersistenceModel asyncSearchPersistenceModel) {
        super(asyncSearchId.getAsyncSearchContextId());
        this.asyncSearchId = asyncSearchId;
        this.asyncSearchPersistenceModel = asyncSearchPersistenceModel;
    }

    public AsyncSearchPersistenceModel getAsyncSearchPersistenceModel() {
        return asyncSearchPersistenceModel;
    }

    @Override
    public AsyncSearchId getAsyncSearchId() {
        return asyncSearchId;
    }

    @Override
    public boolean isRunning() {
        return false;
    }

    @Override
    public long getExpirationTimeMillis() {
        return asyncSearchPersistenceModel.getExpirationTimeMillis();
    }

    @Override
    public long getStartTimeMillis() {
        return asyncSearchPersistenceModel.getStartTimeMillis();
    }

    @Override
    public SearchResponse getSearchResponse() {

        BytesReference response = asyncSearchPersistenceModel.getResponse();
        if (response != null) {
            try (XContentParser parser = XContentType.JSON.xContent()
                    .createParser(NamedXContentRegistry.EMPTY, LoggingDeprecationHandler.INSTANCE, response.streamInput())) {
                return SearchResponse.fromXContent(parser);
            } catch (IOException e) {
                logger.debug(new ParameterizedMessage("could not parse search response for async search id : {}",
                        AsyncSearchId.buildAsyncId(asyncSearchId), e));
                return null;
            }
        } else {
            return null;
        }
    }

    @Override
    public ElasticsearchException getSearchError() {

        BytesReference error = asyncSearchPersistenceModel.getError();
        if (error != null) {
            try (XContentParser parser = XContentType.JSON.xContent()
                    .createParser(NamedXContentRegistry.EMPTY, LoggingDeprecationHandler.INSTANCE, error.streamInput())) {
                return ElasticsearchException.fromXContent(parser);
            } catch (IOException e) {
                logger.debug(() -> new ParameterizedMessage("could not parse search error for async search id : {}",
                        AsyncSearchId.buildAsyncId(asyncSearchId), e));
                return null;
            }

        } else {
            return null;
        }
    }

    @Override
    public Stage getStage() {
        return Stage.PERSISTED;
    }

    @Override
    public int hashCode() {
        return Objects.hash(asyncSearchId, asyncSearchPersistenceModel);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;
        AsyncSearchPersistenceContext asyncSearchPersistenceContext = (AsyncSearchPersistenceContext) o;
        return asyncSearchPersistenceContext.getAsyncSearchId()
                .equals(this.asyncSearchId) && asyncSearchPersistenceContext.getAsyncSearchPersistenceModel()
                .equals(this.asyncSearchPersistenceModel);
    }
}
