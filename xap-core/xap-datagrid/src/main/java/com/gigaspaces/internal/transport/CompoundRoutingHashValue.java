package com.gigaspaces.internal.transport;


import com.gigaspaces.internal.io.IOUtils;
import com.gigaspaces.serialization.SmartExternalizable;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

public class CompoundRoutingHashValue implements SmartExternalizable {
    int hash = 0;

    public CompoundRoutingHashValue() {

    }

    public void concatValue(Object routingValue) {
        hash = 31 * hash + routingValue.hashCode();
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        IOUtils.writeInt(out, hash);
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        this.hash = IOUtils.readInt(in);
    }


    @Override
    public int hashCode() {
        return hash;
    }
}