package com.amazon.opendistroforelasticsearch.search.async;

import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.ObjectParser;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;

import java.io.IOException;
import java.util.Objects;

import static org.elasticsearch.common.xcontent.ConstructingObjectParser.constructorArg;

/**
 * A response that indicates that a request has been acknowledged
 */
public class AcknowledgedResponse extends ActionResponse implements ToXContentObject {

    private static final ParseField ACKNOWLEDGED = new ParseField("acknowledged");

    protected static <T extends AcknowledgedResponse> void declareAcknowledgedField(ConstructingObjectParser<T, Void> objectParser) {
        objectParser.declareField(constructorArg(), (parser, context) -> parser.booleanValue(), ACKNOWLEDGED,
                ObjectParser.ValueType.BOOLEAN);
    }

    protected boolean acknowledged;

    public AcknowledgedResponse(StreamInput in) throws IOException {
        super(in);
        acknowledged = in.readBoolean();
    }

    public AcknowledgedResponse(StreamInput in, boolean readAcknowledged) throws IOException {
        super(in);
        if (readAcknowledged) {
            acknowledged = in.readBoolean();
        }
    }

    public AcknowledgedResponse(boolean acknowledged) {
        this.acknowledged = acknowledged;
    }

    /**
     * Returns whether the response is acknowledged or not
     * @return true if the response is acknowledged, false otherwise
     */
    public final boolean isAcknowledged() {
        return acknowledged;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeBoolean(acknowledged);
    }

    @Override
    public final XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(ACKNOWLEDGED.getPreferredName(), isAcknowledged());
        addCustomFields(builder, params);
        builder.endObject();
        return builder;
    }

    protected void addCustomFields(XContentBuilder builder, Params params) throws IOException {

    }

    /**
     * A generic parser that simply parses the acknowledged flag
     */
    private static final ConstructingObjectParser<Boolean, Void> ACKNOWLEDGED_FLAG_PARSER = new ConstructingObjectParser<>(
            "acknowledged_flag", true, args -> (Boolean) args[0]);

    static {
        ACKNOWLEDGED_FLAG_PARSER.declareField(constructorArg(), (parser, context) -> parser.booleanValue(), ACKNOWLEDGED,
                ObjectParser.ValueType.BOOLEAN);
    }

    public static AcknowledgedResponse fromXContent(XContentParser parser) throws IOException {
        return new AcknowledgedResponse(ACKNOWLEDGED_FLAG_PARSER.apply(parser, null));
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        AcknowledgedResponse that = (AcknowledgedResponse) o;
        return isAcknowledged() == that.isAcknowledged();
    }

    @Override
    public int hashCode() {
        return Objects.hash(isAcknowledged());
    }
}
