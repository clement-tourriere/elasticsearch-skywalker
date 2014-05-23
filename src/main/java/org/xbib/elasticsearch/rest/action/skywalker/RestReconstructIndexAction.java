
package org.xbib.elasticsearch.rest.action.skywalker;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestChannel;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.BytesRestResponse;
import org.xbib.elasticsearch.action.admin.indices.reconstruct.ReconstructIndexAction;
import org.xbib.elasticsearch.action.admin.indices.reconstruct.ReconstructIndexRequest;
import org.xbib.elasticsearch.action.admin.indices.reconstruct.ReconstructIndexResponse;
import org.xbib.elasticsearch.action.admin.indices.reconstruct.ShardReconstructIndexResponse;

import java.io.IOException;

import static org.elasticsearch.rest.RestRequest.Method.GET;
import static org.elasticsearch.rest.RestStatus.OK;

/**
 *  REST action for reconstructing an index
 */
public class RestReconstructIndexAction extends BaseRestHandler {

    @Inject
    public RestReconstructIndexAction(Settings settings, Client client, RestController controller) {
        super(settings, client);
        controller.registerHandler(GET, "/{index}/_skywalker/reconstruct", this);
    }

    @Override
    public void handleRequest(final RestRequest request, final RestChannel channel) {
        client.admin().indices().execute(ReconstructIndexAction.INSTANCE,
                new ReconstructIndexRequest(request.param("index")),
                new ActionListener<ReconstructIndexResponse>() {
                    @Override
                    public void onResponse(ReconstructIndexResponse response) {
                        try {
                            XContentBuilder builder = channel.newBuilder();
                            channel.sendResponse(new BytesRestResponse(OK, builder.startObject()
                                    .field("ok", true)
                                    .field("index", request.param("index"))
                                    .startArray("shards")));

                            for (ShardReconstructIndexResponse r : response.shards()) {
                                XContentParser p = XContentHelper.createParser(r.getReconstructedIndex().bytes());
                                builder.copyCurrentStructure(p);
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