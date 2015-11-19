package com.layerx.fenzobrain;

import com.netflix.fenzo.*;
import com.netflix.fenzo.functions.Action1;
import org.apache.mesos.Protos;

import java.io.IOException;
import java.util.*;

public class Brain {
    private static final Map<String, Protos.Offer> pendingOffers = new HashMap<>();
    //    private static final Map<String, Protos.Offer> usedOffers = new HashMap<>();
    private static final Map<String, Protos.TaskInfo> runningTasks = new HashMap<>();
    private static final Map<String, Protos.TaskInfo> pendingTasks = new HashMap<>();

    private static class LayerXException extends Exception {
        public LayerXException(String msg) {
            super(msg);
        }
    }

    public static void notifyOfferReceived(Protos.Offer offer) {
        String offerID = offer.getId().getValue();
        if (pendingOffers.containsKey(offerID)) {
            System.out.println("I already know about offer " + offerID);
        } else {
            pendingOffers.put(offerID, offer);
        }
    }

    public static void notifyTaskReceived(Protos.TaskInfo taskInfo) {
        String taskID = taskInfo.getTaskId().getValue();
        if (pendingTasks.containsKey(taskID)) {
            System.out.println("I already know about task " + taskID);
        } else {
            pendingTasks.put(taskID, taskInfo);
        }
    }

    private static void runTask(BrainClient brainClient, Protos.TaskInfo taskToLaunch, Protos.Offer offer) throws Exception {
        brainClient.scheduleTasks(taskToLaunch, offer);
        runningTasks.put(taskToLaunch.getTaskId().getValue(), taskToLaunch);
    }

    public static TaskScheduler initTaskScheduler(final BrainClient brainClient) {
        return new TaskScheduler.Builder()
                //todo: add more stuff
                .withLeaseRejectAction(new Action1<VirtualMachineLease>() {
                    @Override
                    public void call(VirtualMachineLease lease) {
                        System.out.println("Declining offer on " + lease.hostname());
                        try {
//                            usedOffers.remove(lease.getOffer().getId().getValue());
//                            pendingOffers.remove(lease.getOffer().getId().getValue());
                            brainClient.declineOffer(lease.getOffer());
                        } catch (Exception e) {
                            e.printStackTrace();
                        }
                    }
                })
                .build();
    }

    public static void main(String[] args) throws Exception {
        final BrainClient brainClient = new BrainClient("http://127.0.0.1:3000");
        TaskScheduler taskScheduler = initTaskScheduler(brainClient);

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
                System.out.println("FenzoBrain: There are " + runningTasks.size() + " tasks currently running.");

                Collection<Protos.Offer> newOffers = pendingOffers.values();
                Collection<Protos.TaskInfo> newTasks = pendingTasks.values();

                System.out.println("Brain: scheduling " + newTasks.size() + " tasks across " + newOffers.size() + " offers...");
                Map<String, Protos.TaskInfo> taskInfoMap = new HashMap<>();
                List<TaskRequest> taskRequests = new ArrayList<>();
                for (Protos.TaskInfo taskInfo : newTasks) {
                    TaskRequest taskRequest = FenzoInfo.fromTaskInfo(taskInfo);
                    taskInfoMap.put(taskRequest.getId(), taskInfo);
                    taskRequests.add(taskRequest);
                }
                List<VirtualMachineLease> vmLeases = new ArrayList<>();
                for (Protos.Offer offer : newOffers) {
                    VirtualMachineLease vmLease = FenzoInfo.fromOffer(offer);
                    vmLeases.add(vmLease);
                }
                if (taskRequests.size() < 1 || vmLeases.size() < 1) {
                    System.out.println("Not enough offers or tasks to schedule anything.");
                    Thread.sleep(1000);
                    continue;
                }

                SchedulingResult schedulingResult = taskScheduler.scheduleOnce(taskRequests, vmLeases);
                System.out.println("result=" + schedulingResult);
                Map<String, VMAssignmentResult> resultMap = schedulingResult.getResultMap();
                System.out.println("about to do stuff with result...");
                if (!resultMap.isEmpty()) {
                    System.out.println("result is not empty");
                    for (VMAssignmentResult vmAssignmentResult : resultMap.values()) {
                        System.out.println("parsing vmAssignment Result..." + vmAssignmentResult);
                        List<VirtualMachineLease> leasesUsed = vmAssignmentResult.getLeasesUsed();
                        StringBuilder stringBuilder = new StringBuilder("Sending to Layer-X: " + leasesUsed.get(0).hostname() + " tasks ");
                        for (VirtualMachineLease lease : leasesUsed) {
                            for (TaskAssignmentResult taskAssignmentResult : vmAssignmentResult.getTasksAssigned()) {
                                stringBuilder.append(taskAssignmentResult.getTaskId()).append(", ");
                                Protos.TaskInfo taskToLaunch = taskInfoMap.get(taskAssignmentResult.getTaskId());
                                taskScheduler.getTaskAssigner().call(taskAssignmentResult.getRequest(), lease.hostname());
                                System.out.println(stringBuilder.toString());
                                try {
                                    runTask(brainClient, taskToLaunch, lease.getOffer());
                                } catch (LayerXException e) {
                                    e.printStackTrace();
                                }
                            }
                        }
                    }
                }
                System.out.println("did i do anything?...");

                //reset known stuff
                pendingOffers.clear();
                pendingTasks.clear();
//                taskScheduler.expireAllLeases();
                taskScheduler = initTaskScheduler(brainClient);

                Thread.sleep(1000);
            }
        } catch (InterruptedException | IOException e) {
            System.out.println("Shutting down!");
            throw e;
        }

    }

}
