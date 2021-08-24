package com.gigaspaces.internal.server.space.repartitioning;

public enum ScaleStatus {
    IN_PROGRESS("in progress"),
    SUCCESS("success"),
    FAIL("fail");

    private String status;

    ScaleStatus(String status){
        this.status = status;
    }

    public String getStatus() {
        return status;
    }

    @Override
    public String toString() {
       return status;
    }

    public static ScaleStatus convertToStatus(String status){
        switch (status){
            case "success":
                return ScaleStatus.SUCCESS;
            case "in progress":
                return ScaleStatus.IN_PROGRESS;
            case "fail":
                return ScaleStatus.FAIL;
            default:
                throw new IllegalArgumentException("Status " + status + " doesn't exist");
        }
    }
}
