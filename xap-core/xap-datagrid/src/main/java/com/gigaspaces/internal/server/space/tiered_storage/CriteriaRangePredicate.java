package com.gigaspaces.internal.server.space.tiered_storage;

import com.gigaspaces.internal.server.storage.IEntryData;
import com.gigaspaces.internal.server.storage.ITemplateHolder;
import com.gigaspaces.internal.transport.ITemplatePacket;
import com.j_spaces.core.cache.context.TemplateMatchTier;
import com.j_spaces.jdbc.builder.range.ComposedRange;
import com.j_spaces.jdbc.builder.range.Range;

public class CriteriaRangePredicate implements CachePredicate, InternalCachePredicate {
    private final String typeName;
    private final Range criteria;
    private final boolean isUnion;

    public CriteriaRangePredicate(String typeName, Range criteria) {
        this(typeName, criteria, false);

    }

    public CriteriaRangePredicate(String typeName, Range criteria, boolean isUnion) {
        this.typeName = typeName;
        this.criteria = criteria;
        this.isUnion = isUnion;
    }

    public String getTypeName() {
        return typeName;
    }

    public Range getCriteria() {
        return criteria;
    }

    @Override
    public TemplateMatchTier evaluate(ITemplateHolder template) {
        TemplateMatchTier templateMatchTier = SqliteUtils.getTemplateMatchTier(criteria, template, null);
        return SqliteUtils.evaluateByMatchTier(template, templateMatchTier);
    }

    //For tests
    @Override
    public TemplateMatchTier evaluate(ITemplatePacket packet) {
        return SqliteUtils.getTemplateMatchTier(criteria, packet, null);
    }

    @Override
    public boolean evaluate(IEntryData entryData) {
        boolean result = false;
        if (criteria instanceof ComposedRange) {
            for (Range range : ((ComposedRange) criteria).getRanges()) {
                if (range instanceof ComposedRange) {
                    result = evalComposedPredicate((ComposedRange) range, entryData);
                } else {
                    Object value = (entryData.getFixedPropertyValue(entryData.getSpaceTypeDescriptor().getFixedPropertyPosition((range.getPath()))));
                    result = range.getPredicate().execute(value);
                }
                if (result && ((ComposedRange) criteria).isUnion())
                    return true;
                if (!result && !((ComposedRange) criteria).isUnion())
                    return false;
            }

        } else {
            Object field = (entryData.getFixedPropertyValue(entryData.getSpaceTypeDescriptor().getFixedPropertyPosition((((Range) criteria).getPath()))));
            result = criteria.getPredicate().execute(field);
        }
        return result;
    }

    @Override
    public String toString() {
        return "CriteriaPredicate{" +
                "typeName='" + typeName + '\'' +
                ", criteria='" + criteria + '\'' +
                '}';
    }

    private boolean evalComposedPredicate(ComposedRange criteria, IEntryData entryData) {

        boolean result = false;
        for (int i = 0; i < criteria.getRanges().size(); i++) {
            Range range = criteria.getRanges().get(i);
            Object value = (entryData.getFixedPropertyValue(entryData.getSpaceTypeDescriptor().getFixedPropertyPosition(range.getPath())));
            result = range.getPredicate().execute(value);
            if (!result && !criteria.isUnion())
                return false;
            if (result && criteria.isUnion())
                return true;

        }
        return result;
    }
}
