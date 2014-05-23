
package org.xbib.elasticsearch.rest.action.skywalker;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.joda.time.Instant;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.SizeUnit;
import org.elasticsearch.common.unit.SizeValue;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.rest.*;
import org.xbib.elasticsearch.action.admin.cluster.state.ConsistencyCheckAction;
import org.xbib.elasticsearch.action.admin.cluster.state.ConsistencyCheckRequest;
import org.xbib.elasticsearch.action.admin.cluster.state.ConsistencyCheckResponse;

import java.io.File;
import java.io.IOException;

import static org.elasticsearch.rest.RestRequest.Method.GET;
import static org.elasticsearch.rest.RestStatus.OK;

/**
 * REST consistency check action
 */
public class RestConsistencyCheckAction extends BaseRestHandler {

    @Inject
    public RestConsistencyCheckAction(Settings settings, Client client, RestController controller) {
        super(settings, client);
        controller.registerHandler(GET, "/_skywalker/consistencycheck", this);
    }

    @Override
    public void handleRequest(final RestRequest request, final RestChannel channel) {
        ConsistencyCheckRequest r = new ConsistencyCheckRequest();
        client.admin().cluster().execute(ConsistencyCheckAction.INSTANCE, r, new ActionListener<ConsistencyCheckResponse>() {

            @Override
            public void onResponse(ConsistencyCheckResponse response) {
                try {
                    XContentBuilder builder = channel.newBuilder();
                    builder.startObject();
                    builder.field("ok", true);
                    builder.startObject("state");
                    response.getState().toXContent(builder, ToXContent.EMPTY_PARAMS);
                    builder.startArray("files");
                    for (File file : response.getFiles()) {
                        Instant instant = new Instant(file.lastModified());
                        builder.startObject()
                            .field("path", file.getAbsolutePath())
                            .field("lastmodified", instant.toDateTime().toString())
                            .field("size", new SizeValue(file.length(), SizeUnit.SINGLE).toString())
                            .field("totalspace", new SizeValue(file.getTotalSpace(), SizeUnit.SINGLE).toString())
                            .field("usablespace", new SizeValue(file.getUsableSpace(), SizeUnit.SINGLE).toString())
                            .field("freespace", new SizeValue(file.getFreeSpace(), SizeUnit.SINGLE).toString())
                        .endObject();
                    }
                    builder.endArray();
                    builder.endObject();
                    channel.sendResponse(new BytesRestResponse(OK, builder));
                } catch (Exception e) {
                    onFailure(e);
                }
            }

            @Override
            public void onFailure(Throwable e) {
                try {
                    logger.error(e.getMessage(), e);
                    channel.sendResponse(new BytesRestResponse(channel, e));
                } catch (IOException e1) {
                    logger.error("Failed to send failure response", e1);
                }
            }
        });
    }
}