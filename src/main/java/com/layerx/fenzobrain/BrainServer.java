package com.layerx.fenzobrain;

import io.netty.buffer.ByteBuf;
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
                        try {
                            ByteBuf buffer = request.content();
                            byte[] body = new byte[buffer.capacity()];
                            for (int i = 0; i < buffer.capacity(); i ++) {
                                byte b = buffer.getByte(i);
                                body[i] = b;
                            }
//                            System.out.println("Received offer: "+ body.toString());
                            Brain.notifyOfferReceived(Protos.Offer.parseFrom(body));
                            response.setStatus(new HttpResponseStatus(202, "offer accepted"));
                            response.respond();
                        }catch (Exception e) {
                            e.printStackTrace();
                            throw e;
                        }
                    }
                })
                .POST(TASK_PATH, new Action() {
                    @Override
                    public void run(Request request, Response response) throws Exception {
                        ByteBuf buffer = request.content();
                        byte[] body = new byte[buffer.capacity()];
                        for (int i = 0; i < buffer.capacity(); i ++) {
                            byte b = buffer.getByte(i);
                            body[i] = b;
                        }
//                        System.out.println("Received task: "+ body.toString());
                        Brain.notifyTaskReceived(Protos.TaskInfo.parseFrom(body));
                        response.setStatus(new HttpResponseStatus(202, "task accepted"));
                        response.respond();
                    }
                })
                .start(3001);
    }

    public static class ServerThread extends Thread {
        private final BrainServer server;
        public ServerThread(BrainServer server){
            this.server = server;
        }
        @Override
        public void run(){
            server.run();
        }
    }
}