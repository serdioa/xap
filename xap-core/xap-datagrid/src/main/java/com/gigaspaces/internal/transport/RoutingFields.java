package com.gigaspaces.internal.transport;


import com.gigaspaces.internal.io.IOUtils;
import com.gigaspaces.serialization.SmartExternalizable;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

public class RoutingFields implements SmartExternalizable {
    int sum = 0;

    public RoutingFields() {

    }

    public void sumValueHashCode(Object routingValue) {
        sum += routingValue.hashCode();
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        IOUtils.writeInt(out, sum);
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        this.sum = IOUtils.readInt(in);
    }


    @Override
    public int hashCode() {
        return sum;
    }
}