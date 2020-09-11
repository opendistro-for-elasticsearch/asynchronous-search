package com.amazon.opendistroforelasticsearch.search.async;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.Version;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.persistent.*;
import org.elasticsearch.threadpool.ThreadPool;

import java.io.IOException;
import java.util.Objects;
import java.util.Optional;


import static org.elasticsearch.common.xcontent.ConstructingObjectParser.constructorArg;

public class AsyncSearchReaperPersistentTaskExecutor extends PersistentTasksExecutor<AsyncSearchReaperPersistentTaskExecutor.TestParams> {

    public static final String NAME = "cluster:admin/persistent/test";

    private static final Logger logger = LogManager.getLogger(AsyncSearchReaperPersistentTaskExecutor.class);

    private final ClusterService clusterService;
    private final Client client;

    public AsyncSearchReaperPersistentTaskExecutor(ClusterService clusterService, Client client) {
        super(NAME, ThreadPool.Names.GENERIC);
        this.clusterService = clusterService;
        this.client = client;
    }

    @Override
    protected void nodeOperation(AllocatedPersistentTask task, AsyncSearchReaperPersistentTaskExecutor.TestParams params, PersistentTaskState state) {
        logger.info("started node operation for the task {}", task);
    }

    public static class TestParams implements PersistentTaskParams {

        public static final ConstructingObjectParser<TestParams, Void> REQUEST_PARSER =
                new ConstructingObjectParser<>(AsyncSearchReaperPersistentTaskExecutor.NAME, args -> new TestParams((String) args[0]));

        static {
            REQUEST_PARSER.declareString(constructorArg(), new ParseField("param"));
        }

        private final Version minVersion;
        private final Optional<String> feature;

        private String executorNodeAttr = null;

        private String responseNode = null;

        private String testParam = null;

        public TestParams() {
            this((String)null);
        }

        public TestParams(String testParam) {
            this(testParam, Version.CURRENT, Optional.empty());
        }

        public TestParams(String testParam, Version minVersion, Optional<String> feature) {
            this.testParam = testParam;
            this.minVersion = minVersion;
            this.feature = feature;
        }

        public TestParams(StreamInput in) throws IOException {
            executorNodeAttr = in.readOptionalString();
            responseNode = in.readOptionalString();
            testParam = in.readOptionalString();
            minVersion = Version.readVersion(in);
            feature = Optional.ofNullable(in.readOptionalString());
        }

        @Override
        public String getWriteableName() {
            return AsyncSearchReaperPersistentTaskExecutor.NAME;
        }

        public void setExecutorNodeAttr(String executorNodeAttr) {
            this.executorNodeAttr = executorNodeAttr;
        }

        public void setTestParam(String testParam) {
            this.testParam = testParam;
        }

        public String getExecutorNodeAttr() {
            return executorNodeAttr;
        }

        public String getTestParam() {
            return testParam;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeOptionalString(executorNodeAttr);
            out.writeOptionalString(responseNode);
            out.writeOptionalString(testParam);
            Version.writeVersion(minVersion, out);
            out.writeOptionalString(feature.orElse(null));
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            builder.field("param", testParam);
            builder.endObject();
            return builder;
        }

        public static TestParams fromXContent(XContentParser parser) throws IOException {
            return REQUEST_PARSER.parse(parser, null);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            TestParams that = (TestParams) o;
            return Objects.equals(executorNodeAttr, that.executorNodeAttr) &&
                    Objects.equals(responseNode, that.responseNode) &&
                    Objects.equals(testParam, that.testParam);
        }

        @Override
        public int hashCode() {
            return Objects.hash(executorNodeAttr, responseNode, testParam);
        }

        @Override
        public Version getMinimalSupportedVersion() {
            return minVersion;
        }

        @Override
        public Optional<String> getRequiredFeature() {
            return feature;
        }
    }
}
