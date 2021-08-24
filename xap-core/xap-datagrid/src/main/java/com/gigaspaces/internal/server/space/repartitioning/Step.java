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
    UNQUIESCE("unquiesce");


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
                return Step.Start;
            case 1:
                return Step.Quiesce;
            case 2:
                return Step.Drain;
            case 3:
                return Step.CREATE_NEW_TOPOLOGY;
            case 4:
                return Step.CREATE_INSTANCES;
            case 5:
                return Step.COPY_CHUNKS;
            case 6:
                return Step.DELETE_CHUNKS;
            case 7:
                return Step.SET_NEW_TOPOLOGY;
            case 8:
                return Step.INFORM_TOPOLOGY_CHANGE;
            case 9:
                return Step.UNQUIESCE;
            default:
                throw new IllegalArgumentException("Step in order " + ordinal + " doesn't exist");
        }
    }
    public static Step convertToStep(String step) {
        switch (step){
            case "start":
                return Step.Start;
            case "quiesce":
                return Step.Quiesce;
            case "drain":
                return Step.Drain;
            case "create-new-topology":
                return Step.CREATE_NEW_TOPOLOGY;
            case "create-instances":
                return Step.CREATE_INSTANCES;
            case "copy-chunks":
                return Step.COPY_CHUNKS;
            case "delete-chunks":
                return Step.DELETE_CHUNKS;
            case "set-new-topology":
                   return Step.SET_NEW_TOPOLOGY;
            case "inform-topology-change":
                return Step.INFORM_TOPOLOGY_CHANGE;
            case "unquiesce":
                return Step.UNQUIESCE;
            default:
                throw new IllegalArgumentException("Step " + step + " doesn't exist");
        }
    }

    @Override
    public String toString(){
        return name;
    }
}
