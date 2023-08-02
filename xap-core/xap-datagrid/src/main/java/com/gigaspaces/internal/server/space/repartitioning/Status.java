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
