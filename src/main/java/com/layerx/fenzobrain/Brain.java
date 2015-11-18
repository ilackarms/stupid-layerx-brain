package com.layerx.fenzobrain;

import com.netflix.fenzo.*;
import com.netflix.fenzo.functions.Action1;
import org.apache.mesos.Protos;

import java.io.IOException;
import java.util.*;

public class Brain {
    private static final Map<String, Protos.Offer> pendingOffers = new HashMap<>();
    private static final Map<String, Protos.Offer> usedOffers = new HashMap<>();
    private static final Map<String, Protos.TaskInfo> runningTasks = new HashMap<>();
    private static final Map<String, Protos.TaskInfo> pendingTasks = new HashMap<>();

    public static void notifyOfferReceived(Protos.Offer offer){
        String offerID = offer.getId().getValue();
        if (pendingOffers.containsKey(offerID)) {
            System.out.println("I already know about offer " + offerID);
            return;
        } else {
            pendingOffers.put(offerID, offer);
        }
    }

    public static void notifyTaskReceived(Protos.TaskInfo taskInfo){
        String taskID = taskInfo.getTaskId().getValue();
        if (pendingTasks.containsKey(taskID)) {
            System.out.println("I already know about task " + taskID);
            return;
        } else {
            pendingTasks.put(taskID, taskInfo);
        }
    }

    private static void runTask(BrainClient brainClient, String taskID, String offerID) throws Exception{
        if (!pendingTasks.containsKey(taskID) || !pendingOffers.containsKey(offerID)) {
            System.out.println("I already know about task " + taskID);
            throw new Exception("TRIED TO SCHEDULE SOMETHING THAT DOESNT EXIST");
        } else {
            Protos.Offer offer = pendingOffers.get(offerID);
            Protos.TaskInfo taskInfo = pendingTasks.get(taskID);
            brainClient.scheduleTasks(pendingTasks.get(taskID), pendingOffers.get(offerID));
            runningTasks.put(taskID, taskInfo);
            usedOffers.put(offerID, offer);
            pendingTasks.remove(taskID);
            pendingOffers.remove(offerID);
        }
    }

    public static void main(String[] args) throws Exception {
        final BrainClient brainClient = new BrainClient("http://127.0.0.1:3000");
        TaskScheduler taskScheduler = new TaskScheduler.Builder()
                //todo: add more stuff
                .withLeaseRejectAction(new Action1<VirtualMachineLease>() {
                    @Override
                    public void call(VirtualMachineLease lease) {
                        System.out.println("Declining offer on " + lease.hostname());
                        try {
                            brainClient.declineOffer(lease.getOffer());
                        } catch (Exception e) {
                            e.printStackTrace();
                        }
                    }
                })
                .build();
        final Map<String, String> launchedTasks = new HashMap<>();

        //register with layer-x
        brainClient.register();

        //start listening server
        BrainServer brainServer = new BrainServer();
        brainServer.run();

        //main loop
        try
        {
            while (true) {
                System.out.println("FenzoBrain: I have "+ pendingOffers.size()+" offers sitting in the bank.");
                System.out.println("FenzoBrain: I have "+pendingTasks.size()+" tasks waiting to be scheduled.");
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
                SchedulingResult schedulingResult = taskScheduler.scheduleOnce(taskRequests, vmLeases);
                System.out.println("result=" + schedulingResult);
                Map<String, VMAssignmentResult> resultMap = schedulingResult.getResultMap();
                if (!resultMap.isEmpty()) {
                    for (VMAssignmentResult vmAssignmentResult : resultMap.values()) {
                        List<VirtualMachineLease> leasesUsed = vmAssignmentResult.getLeasesUsed();
                        StringBuilder stringBuilder = new StringBuilder("Sending to Layer-X: " + leasesUsed.get(0).hostname() + " tasks ");
                        for (VirtualMachineLease lease : leasesUsed) {
                            for (TaskAssignmentResult taskAssignmentResult : vmAssignmentResult.getTasksAssigned()) {
                                stringBuilder.append(taskAssignmentResult.getTaskId()).append(", ");
                                Protos.TaskInfo taskToLaunch = taskInfoMap.get(taskAssignmentResult.getTaskId());
                                // remove task from pending tasks map and put into launched tasks map
                                // (in real world, transition the task state)
                                launchedTasks.put(taskAssignmentResult.getTaskId(), lease.hostname());
                                taskScheduler.getTaskAssigner().call(taskAssignmentResult.getRequest(), lease.hostname());
                                System.out.println(stringBuilder.toString());
                                brainClient.scheduleTasks(taskToLaunch, lease.getOffer());
                            }
                        }
                    }
                }

                Thread.sleep(3000);
            }
        }

        catch(InterruptedException|
                IOException e
                )

        {
            System.out.println("Shutting down!");
            throw e;
        }

    }

}
