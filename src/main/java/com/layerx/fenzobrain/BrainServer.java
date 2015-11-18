package com.layerx.fenzobrain;

import io.netty.handler.codec.http.HttpResponseStatus;
import org.apache.mesos.Protos;
import sinetja.Action;
import sinetja.Request;
import sinetja.Response;
import sinetja.Server;

public class BrainServer  {
    private static final String OFFER_PATH="/notify_offer";
    private static final String TASK_PATH="/notify_task";

    public void run() {
        new Server()
                .POST(OFFER_PATH, new Action() {
                    @Override
                    public void run(Request request, Response response) throws Exception {
                        byte[] body = request.content().array();
                        Brain.notifyOfferReceived(Protos.Offer.parseFrom(body));
                        response.setStatus(new HttpResponseStatus(202, "offer accepted"));
                        response.respond();
                    }
                })
                .POST(TASK_PATH, new Action() {
                    @Override
                    public void run(Request request, Response response) throws Exception {
                        byte[] body = request.content().array();
                        Brain.notifyTaskReceived(Protos.TaskInfo.parseFrom(body));
                        response.setStatus(new HttpResponseStatus(202, "task accepted"));
                        response.respond();
                    }
                })
                .start(3001);
    }
}