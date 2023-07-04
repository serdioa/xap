/*
 * Copyright (c) 2008-2016, GigaSpaces Technologies, Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
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
