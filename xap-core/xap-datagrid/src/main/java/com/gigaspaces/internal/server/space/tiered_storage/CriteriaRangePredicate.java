package com.gigaspaces.internal.server.space.tiered_storage;

import com.gigaspaces.internal.server.storage.IEntryData;
import com.gigaspaces.internal.server.storage.ITemplateHolder;
import com.gigaspaces.internal.transport.ITemplatePacket;
import com.j_spaces.core.cache.context.TemplateMatchTier;
import com.j_spaces.jdbc.builder.range.ComposedRange;
import com.j_spaces.jdbc.builder.range.Range;

import java.util.ArrayList;

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
        int j = 0;
        if (criteria instanceof ComposedRange) {
            for (ArrayList path : ((ComposedRange) criteria).getPaths()) {
                for (int i = 0; i < path.size(); i++) {
                    Object field = (entryData.getFixedPropertyValue(entryData.getSpaceTypeDescriptor().getFixedPropertyPosition((String) path.get(i))));
                    result = ((ComposedRange) criteria).get_ranges().get(j).getPredicate().execute(field);
                    if (!result && !((ComposedRange) criteria).isUnion())
                        return false;
                    if (result && ((ComposedRange) criteria).isUnion())
                        return true;
                }
                j++;
            }
            //check what happens when last element - maybe if last- return last element's result.
        } else {
            Object field = (entryData.getFixedPropertyValue(entryData.getSpaceTypeDescriptor().getFixedPropertyPosition((((Range) criteria).getPath()))));
            result = ((ComposedRange) criteria).get_ranges().get(0).getPredicate().execute(field);

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

    private boolean evalComposedPredicate(IEntryData entryData) {

        int i = 0;
        boolean result = false;
        for (ArrayList path : ((ComposedRange) criteria).getPaths()) {
            for (i = 0; i < path.size(); i++) {
                Object field = (entryData.getFixedPropertyValue(entryData.getSpaceTypeDescriptor().getFixedPropertyPosition((String) path.get(i))));
                result = ((ComposedRange) criteria).get_ranges().get(i).getPredicate().execute(field);
                if (!result && !((ComposedRange) criteria).isUnion())
                    return false;
                if (result && ((ComposedRange) criteria).isUnion())
                    return true;
            }
        }
        return true;
    }
}
