package fenzobrain;

import com.netflix.fenzo.ConstraintEvaluator;
import com.netflix.fenzo.TaskRequest;
import com.netflix.fenzo.VMTaskFitnessCalculator;
import com.netflix.fenzo.VirtualMachineLease;
import org.apache.mesos.Protos;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class FenzoInfo {
    public static TaskRequest fromTaskInfo(final Protos.TaskInfo taskInfo){
        return new TaskRequest() {
            @Override
            public String getId() {
                return taskInfo.getTaskId().getValue();
            }

            @Override
            public String taskGroupName() {
                return "";
            }

            @Override
            public double getCPUs() {
                return getCpusFrom(taskInfo);
            }

            @Override
            public double getMemory() {
                return getMemFrom(taskInfo);
            }

            @Override
            public double getNetworkMbps() {
                return 0;
            }

            @Override
            public double getDisk() {
                return getDiskFrom(taskInfo);
            }

            @Override
            public int getPorts() {
                return getPortsFrom(taskInfo);
            }

            @Override
            public List<? extends ConstraintEvaluator> getHardConstraints() {
                return null;
            }

            @Override
            public List<? extends VMTaskFitnessCalculator> getSoftConstraints() {
                return null;
            }
        };
    }

    public static VirtualMachineLease fromOffer(final Protos.Offer offer){
        return new VirtualMachineLease() {
            @Override
            public String getId() {
                return offer.getId().getValue();
            }

            @Override
            public long getOfferedTime() {
                return 0;
            }

            @Override
            public String hostname() {
                return offer.getHostname();
            }

            @Override
            public String getVMID() {
                return offer.getSlaveId().getValue();
            }

            @Override
            public double cpuCores() {
                return getCpusFrom(offer);
            }

            @Override
            public double memoryMB() {
                return getMemFrom(offer);
            }

            @Override
            public double networkMbps() {
                return 0;
            }

            @Override
            public double diskMB() {
                return getDiskFrom(offer);
            }

            @Override
            public List<Range> portRanges() {
                return getPortsFrom(offer);
            }

            @Override
            public Protos.Offer getOffer() {
                return offer;
            }

            @Override
            public Map<String, Protos.Attribute> getAttributeMap() {
                return getAttributeMapFrom(offer);
            }
        };
    }

    private static double getCpusFrom(Protos.Offer offer){
        for (Protos.Resource resource: offer.getResourcesList()){
            if (resource.getName() == "cpus") {
                return resource.getScalar().getValue();
            }
        }
        return -1;
    }

    private static double getMemFrom(Protos.Offer offer){
        for (Protos.Resource resource: offer.getResourcesList()){
            if (resource.getName() == "mem") {
                return resource.getScalar().getValue();
            }
        }
        return -1;
    }

    private static double getDiskFrom(Protos.Offer offer){
        for (Protos.Resource resource: offer.getResourcesList()){
            if (resource.getName() == "disk") {
                return resource.getScalar().getValue();
            }
        }
        return -1;
    }

    private static Map<String, Protos.Attribute> getAttributeMapFrom(Protos.Offer offer){
        Map<String, Protos.Attribute> attributeMap = new HashMap<>();
        for (Protos.Attribute attribute : offer.getAttributesList()){
            attributeMap.put(attribute.getName(), attribute);
        }
        return attributeMap;
    }

    private static List<VirtualMachineLease.Range> getPortsFrom(Protos.Offer offer){
        List<VirtualMachineLease.Range> ranges = new ArrayList<>();
        for (Protos.Resource resource: offer.getResourcesList()){
            if (resource.getName() == "ports") {
                for (Protos.Value.Range range : resource.getRanges().getRangeList()){
                    int beg = (int) range.getBegin();
                    int end = (int) range.getEnd();
                    VirtualMachineLease.Range vmRange = new VirtualMachineLease.Range(beg, end);
                    ranges.add(vmRange);
                }
            }
        }
        return ranges;
    }

    private static double getCpusFrom(Protos.TaskInfo taskInfo){
        for (Protos.Resource resource: taskInfo.getResourcesList()){
            if (resource.getName() == "cpus") {
                return resource.getScalar().getValue();
            }
        }
        return -1;
    }

    private static double getMemFrom(Protos.TaskInfo taskInfo){
        for (Protos.Resource resource: taskInfo.getResourcesList()){
            if (resource.getName() == "mem") {
                return resource.getScalar().getValue();
            }
        }
        return -1;
    }

    private static double getDiskFrom(Protos.TaskInfo taskInfo){
        for (Protos.Resource resource: taskInfo.getResourcesList()){
            if (resource.getName() == "disk") {
                return resource.getScalar().getValue();
            }
        }
        return -1;
    }

    private static int getPortsFrom(Protos.TaskInfo taskInfo){
        double portCount = 0;
        for (Protos.Resource resource: taskInfo.getResourcesList()){
            if (resource.getName() == "ports") {
                for (Protos.Value.Range range : resource.getRanges().getRangeList()){
                    portCount = 1 + range.getEnd() - range.getBegin();
                }
            }
        }
        return (int) portCount;
    }
}
