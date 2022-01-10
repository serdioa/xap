package com.gigaspaces.entry;

import com.gigaspaces.internal.io.IOUtils;
import com.gigaspaces.internal.utils.GsEnv;
import com.gigaspaces.serialization.SmartExternalizable;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Arrays;

/**
 * This Object was created in order to implement GigaSpaces CompoundId logic
 * Extending this object will result in inherit all the capabilities of CompoundSpaceId
 * In order to use this, please implement your CompoundId pojo as explained:
 *
 * use EmbeddedId hibernate annotation on your getter for the compoundId instance in your POJO that holds the conpoundId
 * use @Embeddable hibernate annotation on your class in order to use this in hivernate
 * Implement your empty constructor with call to super with the number of values you have in your compoundID
 * public MyCompoundSpaceId() {
 *    super(2);
 * }
 *
 * @Column(name = "FIELDKEY1")
 * public String getFieldKey1() {
 *     return (String) getValue(0);
 * }
 * public void setFieldKey1(String fieldKey1) {
 *   setValue(0, fieldKey1);
 * }
 *
 * Author: Ayelet Morris
 * Since 15.5.0
 */
public class CompoundSpaceId implements SmartExternalizable {

    private static final long serialVersionUID = 1L;
    private static final String SEPARATOR = GsEnv.property("com.gs.compound-id-separator").get("|");

    private Object[] values;

    /**
     * Required for Externalizable
     */
    public CompoundSpaceId() {
    }

    public CompoundSpaceId(Object[] values) {
        this.values = values;
    }

    public CompoundSpaceId(int numOfValues) {
        this(new Object[numOfValues]);
    }

    public static CompoundSpaceId from(Object ... values) {
        return new CompoundSpaceId(values);
    }

    public int length() {
        return values.length;
    }

    public Object getValue(int index) {
        return values[index];
    }

    public void setValue(int index, Object value) {
        values[index] = value;
    }

    @Override
    public String toString() {
        if (values.length == 0)
            return "";
        if (values.length == 1)
            return values[0].toString();
        StringBuilder sb = new StringBuilder();
        sb.append(values[0]);
        for (int i = 1; i < values.length; i++)
            sb.append(SEPARATOR).append(values[i]);
        return sb.toString();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;
        if (!(o instanceof CompoundSpaceId))
            return false;
        CompoundSpaceId that = (CompoundSpaceId) o;
        return Arrays.equals(that.values,this.values);
    }

    @Override
    public int hashCode() {
        return Arrays.hashCode(values);
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        IOUtils.writeObjectArray(out, values);
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        values = IOUtils.readObjectArray(in);
    }
}
