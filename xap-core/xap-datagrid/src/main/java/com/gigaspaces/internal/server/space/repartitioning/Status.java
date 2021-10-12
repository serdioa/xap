package com.gigaspaces.internal.server.space.repartitioning;

public enum Status {
    IN_PROGRESS("in progress"),
    SUCCESS("success"),
    FAIL("fail");

    private String status;

    Status(String status){
        this.status = status;
    }

    public String getStatus() {
        return status;
    }

    @Override
    public String toString() {
       return status;
    }

    public static Status convertToStatus(String status){
        switch (status){
            case "success":
                return Status.SUCCESS;
            case "in progress":
                return Status.IN_PROGRESS;
            case "fail":
                return Status.FAIL;
            default:
                throw new IllegalArgumentException("Status " + status + " doesn't exist");
        }
    }
}
