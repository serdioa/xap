package com.j_spaces.jdbc.builder.range;

import com.gigaspaces.internal.metadata.ITypeDesc;
import com.gigaspaces.internal.query.predicate.ISpacePredicate;
import com.gigaspaces.internal.query.predicate.comparison.*;
import com.j_spaces.core.client.SQLQuery;
import com.j_spaces.core.client.TemplateMatchCodes;
import com.j_spaces.jdbc.builder.QueryTemplatePacket;

import static com.j_spaces.core.client.TemplateMatchCodes.*;
import static com.j_spaces.core.client.TemplateMatchCodes.GT;

@com.gigaspaces.api.InternalApi
public class TwoColumnsSegmentRange extends Range {
    // serialVersionUID should never be changed.
    private static final long serialVersionUID = 1L;

    //private Comparable _min;
    private boolean _includeMin;
    //private Comparable _max;
    private boolean _includeMax;
    private short _code;

    public TwoColumnsSegmentRange() {
        super();
    }

    @Override
    public Range intersection(Range range) {
        return null;
    }

    @Override
    public Range intersection(IsNullRange range) {
        return null;
    }

    @Override
    public Range intersection(NotNullRange range) {
        return null;
    }

    @Override
    public Range intersection(EqualValueRange range) {
        return null;
    }

    @Override
    public Range intersection(NotEqualValueRange range) {
        return null;
    }

    @Override
    public Range intersection(RegexRange range) {
        return null;
    }

    @Override
    public Range intersection(NotRegexRange range) {
        return null;
    }

    @Override
    public Range intersection(SegmentRange range) {
        return null;
    }

    @Override
    public Range intersection(InRange range) {
        return null;
    }

    @Override
    public Range intersection(RelationRange range) {
        return null;
    }

    /* (non-Javadoc)
     * @see com.j_spaces.jdbc.builder.range.Range#toExternalEntry(com.j_spaces.core.client.ExternalEntry, int)
     */
    public void toEntryPacket(QueryTemplatePacket e, int index) {
        //if (getMin() == null) {
            //e.setFieldValue(index, getMax());
            //e.setExtendedMatchCode(index, _includeMax ? LE : LT);
        //} else if (getMax() == null) {
          //  e.setFieldValue(index, getMin());
          //  e.setExtendedMatchCode(index, _includeMin ? GE : GT);
        //} else {
            //e.setFieldValue(index, getMin());
            //e.setExtendedMatchCode(index, _includeMin ? GE : GT);
            //e.setRangeValue(index, getMax());
            //e.setRangeValueInclusion(index, _includeMax);
        //}
        //e.setFieldValue(index, /*getMin()*/null);
        //todo change to position + add between logic?
        e.setExtendedMatchCodeColumns(index, getPath2());
        e.setExtendedMatchCode(index, _code);
    }

    public TwoColumnsSegmentRange(String colNameLeft, String colNameRight, boolean includeMin, boolean includeMax, short code) {
        this(colNameLeft, null, colNameRight, includeMin, includeMax, code);
    }

    public TwoColumnsSegmentRange(String colNameLeft, FunctionCallDescription functionCallDescription, String colNameRight, boolean includeMin,
                                  boolean includeMax, short code) {
        super(colNameLeft, functionCallDescription, createSpacePredicate(includeMin, includeMax, code), colNameRight);

        _includeMin = includeMin;
        _includeMax = includeMax;
        _code = code;
    }

    private static ISpacePredicate createSpacePredicate(boolean includeMin, boolean includeMax, short code) {
        /*if (value1 != null && value2 != null)
            return new BetweenSpacePredicate(value1, value2, includeMin, includeMax);*/

        if (code == GT || code == GE)
            return includeMin ? new GreaterEqualsSpacePredicate(95) : new GreaterSpacePredicate(95);

        if (code == LT || code == LE)
            return includeMax ? new LessEqualsSpacePredicate(95) : new LessSpacePredicate(95);

        throw new IllegalArgumentException("Not supported predicate with code " + code);
    }


    @Override
    public SQLQuery toSQLQuery(ITypeDesc typeDesc) {
        return null;
    }
}
