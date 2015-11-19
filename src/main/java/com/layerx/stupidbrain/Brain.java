package com.layerx.stupidbrain;

import org.apache.mesos.Protos;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

public class Brain {
    private static final BlockingQueue<Protos.Offer> pendingOffers = new LinkedBlockingQueue<>();
    private static final BlockingQueue<Protos.TaskInfo> pendingTasks = new LinkedBlockingQueue<>();
    private static final Map<String, String> knownTaskIds = new HashMap<>();
    private static final Map<String, String> knownOfferIds = new HashMap<>();

    private static class LayerXException extends Exception {
        public LayerXException(String msg) {
            super(msg);
        }
    }

    private static Boolean offerKnown(Protos.Offer offer){
        return knownOfferIds.containsKey(offer.getId().getValue());
    }

    private static Boolean taskKnown(Protos.TaskInfo taskInfo){
        return knownTaskIds.containsKey(taskInfo.getTaskId().getValue());
    }

    public static void notifyOfferReceived(Protos.Offer offer) {
//        System.out.println("OFFER RECEIVED: "+offer.getId().getValue());
//        System.out.println("OFFER RECEIVED values: "+offer.toString());
        String offerID = offer.getId().getValue();
        if (offerKnown(offer)) {
            System.out.println("I already know about offer " + offerID);
        } else {
            pendingOffers.offer(offer);
            knownOfferIds.put(offerID, offerID);
        }
    }

    public static void notifyTaskReceived(Protos.TaskInfo taskInfo) {
//        System.out.println("TASK RECEIVED values: "+taskInfo.toString());
        String taskID = taskInfo.getTaskId().getValue();
        if (taskKnown(taskInfo)) {
            System.out.println("I already know about task " + taskID);
        } else {
            pendingTasks.offer(taskInfo);
            knownTaskIds.put(taskID, taskID);
        }
    }

    private static void runTask(BrainClient brainClient, Protos.TaskInfo taskToLaunch, Protos.Offer acceptedOffer) throws Exception {
        System.out.println("Putting " + taskToLaunch.getTaskId().getValue() + " on " + acceptedOffer.getId().getValue());
        brainClient.scheduleTasks(taskToLaunch, acceptedOffer);
        knownTaskIds.remove(taskToLaunch.getTaskId().getValue());
        knownOfferIds.remove(acceptedOffer.getId().getValue());
    }

    public static void main(String[] args) throws Exception {
        final BrainClient brainClient = new BrainClient("http://127.0.0.1:3000");

        //register with layer-x
        brainClient.register();

        //start listening server
        BrainServer brainServer = new BrainServer();
        BrainServer.ServerThread runServerThread = new BrainServer.ServerThread(brainServer);
        System.out.println("Starting server...: ");
        runServerThread.start();
        System.out.println("Server started...: ");
        //main loop
        try {
            while (true) {
                System.out.println("FenzoBrain: I have " + pendingOffers.size() + " offers sitting in the bank.");
                System.out.println("FenzoBrain: I have " + pendingTasks.size() + " tasks waiting to be scheduled.");
                Thread.sleep(500);

                Protos.TaskInfo taskToLaunch = pendingTasks.poll();
                if (taskToLaunch == null) {
                    System.out.println("No more tasks to launch.");
                    knownTaskIds.clear();
                    continue;
                }
                Protos.Offer chosenOffer = pendingOffers.poll();
                if (chosenOffer == null) {
                    System.out.println("Don't have any offers to launch task "+taskToLaunch.getTaskId().getValue()+" on. Waiting...");
                    knownOfferIds.clear();
                    continue;
                }
                if (StupidMatcher.matchOffer(taskToLaunch, chosenOffer)){
                    runTask(brainClient, taskToLaunch, chosenOffer);
                }
            }
        } catch (InterruptedException e) {
            System.out.println("Shutting down!");
        }
    }
}
