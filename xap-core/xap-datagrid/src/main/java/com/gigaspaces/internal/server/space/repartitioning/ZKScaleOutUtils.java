package com.gigaspaces.internal.server.space.repartitioning;

import com.gigaspaces.admin.quiesce.QuiesceToken;
import com.gigaspaces.attribute_store.AttributeStore;
import com.gigaspaces.internal.zookeeper.ZNodePathFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

public class ZKScaleOutUtils {
    private static Logger logger = LoggerFactory.getLogger("com.gigaspaces.internal.server.space.repartitioning.ZookeeperScaleOutUtils");

    public static void setScaleOutDetails(AttributeStore attributeStore, String puName, String key, String value) throws IOException {
        attributeStore.set(ZKScaleOutUtils.getScaleOutPath(puName) + "/" + key, value);
    }

    public static void setQuiesceToken(AttributeStore attributeStore, String puName, Object value) throws IOException {
        attributeStore.setObject(ZKScaleOutUtils.getScaleOutPath(puName) + "/quiesce-token", value);
    }

    public static QuiesceToken getQuiesceToken(AttributeStore attributeStore, String puName) throws IOException {
        return attributeStore.getObject(ZKScaleOutUtils.getScaleOutPath(puName) + "/quiesce-token");
    }

    public static String getScaleOutDetails(AttributeStore attributeStore, String puName, String key) throws IOException {
        return attributeStore.get(ZKScaleOutUtils.getScaleOutPath(puName) + "/" + key);
    }

    public static String getScaleOutLastStep(AttributeStore attributeStore, String puName) throws IOException {
        return attributeStore.get(ZKScaleOutUtils.getScaleOutPath(puName) + "/last-step");
    }

    public static List<Integer> getSourcePartitions(AttributeStore attributeStore, String puName) throws IOException {
        String sourcePartitions = getScaleOutDetails(attributeStore, puName, "participating instances");
        String[] sources = sourcePartitions.split(", ");
        return Arrays.stream(sources).map(Integer::parseInt).collect(Collectors.toList());
    }

    public static void setStepIfPossible(AttributeStore attributeStore, String puName, String step, String key, String value)  {
        try{
            attributeStore.set(ZKScaleOutUtils.getScaleStepsPath(puName, step) + "/" + key, value);
        } catch (IOException e){
           logger.warn("Failed to set on Zookeeper: " + key + " " + value , e);
        }
    }

    public static boolean isScaleInProgress(AttributeStore attributeStore, String puName){//todo- change name
       try {
           String status = getScaleOutDetails(attributeStore, puName, "status");
           if (status != null){
               logger.info("Scale status for pu ["  + puName + "] is [" + status + "]");
               return ScaleStatus.IN_PROGRESS.getStatus().equals(status);
           }
       } catch (IOException e) {
       }
        return false;
    }

    public static boolean isScaleIsSucceeded(AttributeStore attributeStore, String puName){//todo- change name
        try {
            String status = getScaleOutDetails(attributeStore, puName, "status");
            if (status != null){
                logger.info("Scale status for pu ["  + puName + "] is [" + status + "]");
                return ScaleStatus.SUCCESS.getStatus().equals(status);
            }
        } catch (IOException e) {
        }
        return false;
    }

    public static String getStepDetails(AttributeStore attributeStore, String puName, String step, String key) throws IOException {
        return attributeStore.get(ZKScaleOutUtils.getScaleStepsPath(puName, step) + "/" + key);
    }

    public static String getScaleOutPath(String puName){
        return ZNodePathFactory.processingUnit(puName, "scale-out");
    }

    public static String getScaleStepsPath(String puName, String step){
        return getScaleOutPath(puName) + "/steps/" + step;
    }
}
