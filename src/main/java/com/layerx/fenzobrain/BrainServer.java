package com.layerx.fenzobrain;

import org.apache.mesos.Protos;
import sinetja.Action;
import sinetja.Request;
import sinetja.Response;
import sinetja.Server;

public class BrainServer  {
    public void run() {
        new Server()
                .POST("/notify_offer", new Action() {
                    @Override
                    public void run(Request request, Response response) throws Exception {
                        byte[] body = request.content().array();
                        Brain.notifyOfferReceived(Protos.Offer.parseFrom(body));
                    }
                })
                .POST("/notify_task", new Action() {
                    @Override
                    public void run(Request request, Response response) throws Exception {
                        byte[] body = request.content().array();
                        Brain.notifyTaskReceived(Protos.TaskInfo.parseFrom(body));
                    }
                })
                .start(3001);
    }
}