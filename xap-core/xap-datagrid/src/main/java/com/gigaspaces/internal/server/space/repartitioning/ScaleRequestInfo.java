package com.gigaspaces.internal.server.space.repartitioning;

public class ScaleRequestInfo {
    private String id;
    private String puName;
    private String description;
    private Throwable error;
    private String completedAt;
    private boolean isCanceled;
    private String status;
    private boolean isCompleted;

    public ScaleRequestInfo() {
    }

    public ScaleRequestInfo(String id, String puName, String description, Throwable error, String completedAt,
                            boolean isCanceled, String status, boolean isCompleted) {
        this.id = id;
        this.puName = puName;
        this.description = description;
        this.error = error;
        this.completedAt = completedAt;
        this.isCanceled = isCanceled;
        this.status = status;
        this.isCompleted = isCompleted;
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getDescription() {
        return description;
    }

    public void setDescription(String description) {
        this.description = description;
    }

    public Throwable getError() {
        return error;
    }

    public void setError(Throwable error) {
        this.error = error;
    }

    public String getCompletedAt() {
        return completedAt;
    }

    public void setCompletedAt(String completedAt) {
        this.completedAt = completedAt;
    }

    public boolean isCanceled() {
        return isCanceled;
    }

    public void setCanceled(boolean canceled) {
        isCanceled = canceled;
    }

    public String getStatus() {
        return status;
    }

    public void setStatus(String status) {
        this.status = status;
    }

    public boolean isCompleted() {
        return isCompleted;
    }

    public void setCompleted(boolean completed) {
        isCompleted = completed;
    }

    public String getPuName() {
        return puName;
    }

    public void setPuName(String puName) {
        this.puName = puName;
    }

    @Override
    public String toString() {
        return "ScaleRequestInfo{" +
                "id='" + id + '\'' +
                ", puName='" + puName + '\'' +
                ", description='" + description + '\'' +
                ", error=" + error +
                ", completedAt='" + completedAt + '\'' +
                ", isCanceled=" + isCanceled +
                ", status='" + status + '\'' +
                ", isCompleted=" + isCompleted +
                '}';
    }
}
