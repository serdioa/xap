package com.gigaspaces.internal.server.space.repartitioning;

import com.gigaspaces.admin.quiesce.QuiesceToken;
import com.gigaspaces.attribute_store.AttributeStore;
import com.gigaspaces.internal.cluster.ClusterTopologyState;
import com.gigaspaces.internal.zookeeper.ZNodePathFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

public class ZKScaleOutUtils {
    private static Logger logger = LoggerFactory.getLogger("com.gigaspaces.internal.server.space.repartitioning.ZookeeperScaleOutUtils");

    public static String getScaleOutPath(String puName){
        return ZNodePathFactory.processingUnit(puName, "scale-out");
    }

    public static String getScaleStepsPath(String puName, String step){
        return getScaleOutPath(puName) + "/steps/" + step;
    }

    public static String getStepDetails(AttributeStore attributeStore, String puName, String step, String key) throws IOException {
        return attributeStore.get(ZKScaleOutUtils.getScaleStepsPath(puName, step) + "/" + key);
    }

    public static void setScaleOutMetaData(AttributeStore attributeStore, String puName, String key, String value) throws IOException {
        attributeStore.set(ZKScaleOutUtils.getScaleOutPath(puName) + "/" + key, value);
    }

    public static void setScaleOutLastStep(AttributeStore attributeStore, String puName, String step) throws IOException {
        attributeStore.set(ZKScaleOutUtils.getScaleOutPath(puName) + "/last-step", step);
    }

    public static void setQuiesceToken(AttributeStore attributeStore, String puName, Object value) throws IOException {
        attributeStore.setObject(ZKScaleOutUtils.getScaleOutPath(puName) + "/quiesce-token", value);
    }

    public static QuiesceToken getQuiesceToken(AttributeStore attributeStore, String puName) throws IOException {
        return attributeStore.getObject(ZKScaleOutUtils.getScaleOutPath(puName) + "/quiesce-token");
    }

    public static String getScaleOutMetaData(AttributeStore attributeStore, String puName, String key) throws IOException {
        return attributeStore.get(ZKScaleOutUtils.getScaleOutPath(puName) + "/" + key);
    }

    public static String getScaleOutLastStep(AttributeStore attributeStore, String puName) throws IOException {
        return attributeStore.get(ZKScaleOutUtils.getScaleOutPath(puName) + "/last-step");
    }

    public static List<Integer> getParticipantPartitions(AttributeStore attributeStore, String puName) throws IOException {
        String sourcePartitions = getScaleOutMetaData(attributeStore, puName, "participating instances");
        String[] sources = sourcePartitions.split(", ");
        return Arrays.stream(sources).map(Integer::parseInt).collect(Collectors.toList());
    }

    public static boolean setStepIfPossible(AttributeStore attributeStore, String puName, String step, String key, String value)  {
        try{
            attributeStore.set(ZKScaleOutUtils.getScaleStepsPath(puName, step) + "/" + key, value);
            return true;
        } catch (IOException e){
           logger.warn("Failed to set on Zookeeper: " + key + " " + value , e);
           return false;
        }
    }

    public static boolean isScaleInProgress(AttributeStore attributeStore, String puName){
       try {
           String status = getScaleOutMetaData(attributeStore, puName, "scale-status");
           if (status != null){
               return Status.IN_PROGRESS.getStatus().equals(status);
           }
       } catch (IOException e) {
       }
        return false;
    }

    public static Status getScaleStatusOnCompletion(AttributeStore attributeStore, String puName){
        try {
            String status = getScaleOutMetaData(attributeStore, puName, "scale-status");
            if (status != null){
                Status result =Status.convertToStatus(status);
                if(Status.SUCCESS.equals(result) || Status.CANCELLED_SUCCESSFULLY.equals(result)){
                    return result;
                }
            }
        } catch (IOException e) {
        }
        return null;
    }

    public static boolean checkIfScaleIsCanceled(AttributeStore attributeStore, String puName){
        try {
            String isCanceled = getScaleOutMetaData(attributeStore, puName, "cancel");
            if(isCanceled != null){
                return Boolean.parseBoolean(isCanceled);
            }
        } catch (IOException ignored) {
        }
        return false;
    }

    public static void setOldTopologyState(AttributeStore attributeStore, String puName, ClusterTopologyState topologyState) {
        try {
            attributeStore.setObject(ZKScaleOutUtils.getScaleOutPath(puName) + "/old cluster topology state", topologyState);
        } catch (IOException e) {
            if (logger.isErrorEnabled()) logger.error("Failed to set cluster topology state", e);
            throw new UncheckedIOException("Failed to set cluster topology state for pu [" + puName + "]", e);
        }
    }

    public static ClusterTopologyState getOldTopologyState(AttributeStore attributeStore, String puName) {
        try {
            return attributeStore.getObject(ZKScaleOutUtils.getScaleOutPath(puName) + "/old cluster topology state");
        } catch (IOException e) {
            if (logger.isErrorEnabled()) logger.error("Failed to set cluster topology state", e);
            throw new UncheckedIOException("Failed to set cluster topology state for pu [" + puName + "]", e);
        }
    }
}
