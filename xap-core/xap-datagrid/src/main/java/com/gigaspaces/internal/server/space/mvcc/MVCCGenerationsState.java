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
package com.gigaspaces.internal.server.space.mvcc;

import com.gigaspaces.internal.server.space.redolog.storage.bytebuffer.ISwapExternalizable;
import com.gigaspaces.serialization.SmartExternalizable;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.io.Serializable;
import java.util.Collections;
import java.util.HashSet;
import java.util.Objects;
import java.util.Set;

@com.gigaspaces.api.InternalApi
public class MVCCGenerationsState implements SmartExternalizable, ISwapExternalizable, Serializable {
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

    public static MVCCGenerationsState empty() {
        return new MVCCGenerationsState(-1L, -1L, Collections.emptySet());
    }

    public static MVCCGenerationsState revertGenerationState(long generationToRevert) {
        return new MVCCGenerationsState(-1L, -1L, Sets.newHashSet(generationToRevert));
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
    public void writeToSwap(ObjectOutput out) throws IOException {
        writeExternal(out);
    }

    @Override
    public void readFromSwap(ObjectInput in) throws IOException, ClassNotFoundException {
        readExternal(in);
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
