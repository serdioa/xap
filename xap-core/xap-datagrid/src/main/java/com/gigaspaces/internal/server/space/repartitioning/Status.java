package com.gigaspaces.internal.server.space.repartitioning;

public enum Status {
    STARTED("started"),
    IN_PROGRESS("in progress"),
    SUCCESS("success"),
    FAIL("fail"),
    CANCELLED_SUCCESSFULLY("cancelled-successfully");

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
            case "started":
                return Status.STARTED;
            case "success":
                return Status.SUCCESS;
            case "in progress":
                return Status.IN_PROGRESS;
            case "fail":
                return Status.FAIL;
            case "cancelled-successfully":
                return Status.CANCELLED_SUCCESSFULLY;
            default:
                throw new IllegalArgumentException("Status " + status + " doesn't exist");
        }
    }
}
