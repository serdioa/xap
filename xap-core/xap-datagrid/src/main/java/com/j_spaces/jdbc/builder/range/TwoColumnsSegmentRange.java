package com.j_spaces.jdbc.builder.range;

import com.gigaspaces.internal.metadata.ITypeDesc;
import com.j_spaces.core.client.SQLQuery;
import com.j_spaces.jdbc.builder.QueryTemplatePacket;


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
        //todo add between logic?
        e.setExtendedMatchCodeColumns(index, getRightColumnPosition());
        e.setExtendedMatchCode(index, _code);
    }

    public TwoColumnsSegmentRange(String colNameLeft, FunctionCallDescription functionCallDescription, int rightColumnPosition, boolean includeMin,
                                  boolean includeMax, short code) {
        super(colNameLeft, functionCallDescription, null, rightColumnPosition);

        _includeMin = includeMin;
        _includeMax = includeMax;
        _code = code;
    }


    @Override
    public SQLQuery toSQLQuery(ITypeDesc typeDesc) {
        return null;
    }
}
