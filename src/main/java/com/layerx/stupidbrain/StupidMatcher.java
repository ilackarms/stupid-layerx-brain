package com.layerx.stupidbrain;

import org.apache.mesos.Protos;

/**
 * Created by pivotal on 11/19/15.
 */
public class StupidMatcher {
    public static Boolean matchOffer(Protos.TaskInfo taskInfo, Protos.Offer offer){
        return (getCpusFrom(offer) >= getCpusFrom(taskInfo)
                && getMemFrom(offer) >= getMemFrom(taskInfo));
    }

    private static double getCpusFrom(Protos.Offer offer){
        for (Protos.Resource resource: offer.getResourcesList()){
            if (resource.getName().contains("cpus")) {
                return resource.getScalar().getValue();
            }
        }
        return -1;
    }

    private static double getMemFrom(Protos.Offer offer){
        for (Protos.Resource resource: offer.getResourcesList()){
            if (resource.getName().contains("mem")) {
                return resource.getScalar().getValue();
            }
        }
        return -1;
    }

    private static double getDiskFrom(Protos.Offer offer){
        for (Protos.Resource resource: offer.getResourcesList()){
            if (resource.getName().contains("disk")) {
                return resource.getScalar().getValue();
            }
        }
        return -1;
    }


    private static double getCpusFrom(Protos.TaskInfo taskInfo){
        for (Protos.Resource resource: taskInfo.getResourcesList()){
            if (resource.getName().contains("cpus")) {
                return resource.getScalar().getValue();
            }
        }
        return 1000000;
    }

    private static double getMemFrom(Protos.TaskInfo taskInfo){
        for (Protos.Resource resource: taskInfo.getResourcesList()){
            if (resource.getName().contains("mem")) {
                return resource.getScalar().getValue();
            }
        }
        return 1000000;
    }

    private static double getDiskFrom(Protos.TaskInfo taskInfo){
        for (Protos.Resource resource: taskInfo.getResourcesList()){
            if (resource.getName().contains("disk")) {
                return resource.getScalar().getValue();
            }
        }
        return 1000000;
    }

    private static int getPortsFrom(Protos.TaskInfo taskInfo){
        double portCount = 0;
        for (Protos.Resource resource: taskInfo.getResourcesList()){
            if (resource.getName().contains("ports")) {
                for (Protos.Value.Range range : resource.getRanges().getRangeList()){
                    portCount = 1 + range.getEnd() - range.getBegin();
                }
            }
        }
        return (int) portCount;
    }
}
