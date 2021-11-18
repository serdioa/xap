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

public enum Step {
    Start("start"),
    Quiesce("quiesce"),
    Drain("drain"),
    CREATE_NEW_TOPOLOGY("create-new-topology"),
    CREATE_INSTANCES("create-instances"),
    COPY_CHUNKS("copy-chunks"),
    DELETE_CHUNKS("delete-chunks"),
    SET_NEW_TOPOLOGY("set-new-topology"),
    INFORM_TOPOLOGY_CHANGE("inform-topology-change"),
    UNQUIESCE("unquiesce"),
    REVERT_PU_METADATA("revert-pu-metadata"),
    KILL_INSTANCES("kill-instances"),
    REVERT_TOPOLOGY("revert-topology"),
    UNQUIESCE_ON_ROLLBACK("unquiesce-on-rollback");


    private String name;

    Step(String name) {
        this.name = name;
    }

    public String getName() {
        return name;
    }

    public static Step convertToStep(int ordinal) {
        switch (ordinal){
            case 0:
                return Start;
            case 1:
                return Quiesce;
            case 2:
                return Drain;
            case 3:
                return CREATE_NEW_TOPOLOGY;
            case 4:
                return CREATE_INSTANCES;
            case 5:
                return COPY_CHUNKS;
            case 6:
                return DELETE_CHUNKS;
            case 7:
                return SET_NEW_TOPOLOGY;
            case 8:
                return INFORM_TOPOLOGY_CHANGE;
            case 9:
                return UNQUIESCE;
            case 10:
                return REVERT_PU_METADATA;
            case 11:
                return KILL_INSTANCES;
            case 12:
                return REVERT_TOPOLOGY;
            case 13:
                return UNQUIESCE_ON_ROLLBACK;
            default:
                throw new IllegalArgumentException("Step in order " + ordinal + " doesn't exist");
        }
    }
    public static Step convertToStep(String step) {
        switch (step){
            case "start":
                return Start;
            case "quiesce":
                return Quiesce;
            case "drain":
                return Drain;
            case "create-new-topology":
                return CREATE_NEW_TOPOLOGY;
            case "create-instances":
                return CREATE_INSTANCES;
            case "copy-chunks":
                return COPY_CHUNKS;
            case "delete-chunks":
                return DELETE_CHUNKS;
            case "set-new-topology":
                   return SET_NEW_TOPOLOGY;
            case "inform-topology-change":
                return INFORM_TOPOLOGY_CHANGE;
            case "unquiesce":
                return UNQUIESCE;
            case "revert-pu-metadata":
                return REVERT_PU_METADATA;
            case "kill-instances":
                return KILL_INSTANCES;
            case "revert-topology":
                return REVERT_TOPOLOGY;
            case "unquiesce-on-rollback":
                    return UNQUIESCE_ON_ROLLBACK;
            default:
                throw new IllegalArgumentException("Step " + step + " doesn't exist");
        }
    }

    @Override
    public String toString(){
        return name;
    }
}
