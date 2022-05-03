package com.gigaspaces.dih.consumer;

/**
 * @since 16.2.0
 */
public class DIHUtils {

    public static String getDeletedObjectsTableName( String pipelineName ){
        return pipelineName + "_DELETED";
    }
}
