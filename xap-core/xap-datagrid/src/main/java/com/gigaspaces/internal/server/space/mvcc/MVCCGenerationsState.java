package com.gigaspaces.internal.server.space.mvcc;

import com.gigaspaces.serialization.SmartExternalizable;
import com.google.common.collect.ImmutableSet;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.HashSet;
import java.util.Objects;
import java.util.Set;

@com.gigaspaces.api.InternalApi
public class MVCCGenerationsState implements SmartExternalizable {
    private static final long serialVersionUID = 3194738361295624722L;
    private long nextGeneration;
    private long completedGeneration;
    private Set<Long> uncompletedGenerations;

    public MVCCGenerationsState(long nextGeneration, long completedGeneration, Set<Long> uncompletedGenerations) {
        this.nextGeneration = nextGeneration;
        this.completedGeneration = completedGeneration;
        this.uncompletedGenerations = uncompletedGenerations;
    }

    public MVCCGenerationsState() {
    }

    public long getNextGeneration() {
        return nextGeneration;
    }

    public void setNextGeneration(long nextGeneration) {
        this.nextGeneration = nextGeneration;
    }

    public long getCompletedGeneration() {
        return completedGeneration;
    }

    public void setCompletedGeneration(long completedGeneration) {
        this.completedGeneration = completedGeneration;
    }

    public boolean isUncompletedGeneration(long generation) {
        return uncompletedGenerations.contains(generation);
    }

    public void addUncompletedGeneration(long generation) {
        uncompletedGenerations.add(generation);
    }

    public void removeFromUncompletedGenerations(Set<Long> completedSet) {
        uncompletedGenerations.removeAll(completedSet);
    }

    public Set<Long> getCopyOfUncompletedGenerationsSet() {
        return ImmutableSet.copyOf(uncompletedGenerations);
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        out.writeLong(nextGeneration);
        out.writeLong(completedGeneration);
        out.writeInt(uncompletedGenerations.size());
        for (Long unCompletedGeneration : uncompletedGenerations) {
            out.writeLong(unCompletedGeneration);
        }
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        this.nextGeneration = in.readLong();
        this.completedGeneration = in.readLong();
        this.uncompletedGenerations = new HashSet<>();
        final int size = in.readInt();
        for (int i = 0; i < size; i++) {
            this.uncompletedGenerations.add(in.readLong());
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        MVCCGenerationsState that = (MVCCGenerationsState) o;
        return nextGeneration == that.nextGeneration && completedGeneration == that.completedGeneration && Objects.equals(uncompletedGenerations, that.uncompletedGenerations);
    }

    @Override
    public int hashCode() {
        return Objects.hash(nextGeneration, completedGeneration, uncompletedGenerations);
    }

    @Override
    public String toString() {
        return "MVCCGenerationsState{" +
                "nextGeneration=" + nextGeneration +
                ", completedGeneration=" + completedGeneration +
                ", uncompletedGenerations=" + uncompletedGenerations +
                '}';
    }
}
