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

/**
 *
 */
package com.j_spaces.sadapter.datasource;

import com.gigaspaces.internal.metadata.ITypeDesc;
import com.gigaspaces.internal.server.storage.ITemplateHolder;
import com.gigaspaces.internal.transport.IEntryPacket;
import com.gigaspaces.internal.utils.GsEnv;
import com.gigaspaces.start.SystemLocations;
import com.j_spaces.core.client.SQLQuery;
import com.j_spaces.core.client.TemplateMatchCodes;

import java.util.Collections;
import java.util.LinkedList;
import java.util.List;

/**
 * Default implementation for building the SQLQuery.
 *
 * @author anna
 */
@com.gigaspaces.api.InternalApi
public class DefaultSQLQueryBuilder implements SQLQueryBuilder {
    public static final String BIND_PARAMETER = "?";
    // Default is true for xap, false for xap.net.
    public static final boolean ADAPT_POSITIONAL_PARAMETERS = GsEnv.propertyBoolean("com.gs.persist.adapt-positional-sql-parameters")
            .get(!SystemLocations.singleton().isXapNet());
    public static final String OR = " or ";
    public static final String AND = " and ";
    /**
     * Extended match-codes mapping array
     *
     * @see #mapCodeToSign(short)
     */
    private static final String[] extendedMatchCodeMapping = {
            " = ", " != ", " > ", " >= ", " < ", " <= ",
            " is null ", " is not null ", " like ", "[*] = "};


    /**
     * Builds the SQLQuery using given template extended match codes and values.
     *
     * @param typeName the query is built using the provided typeName incase the template represents
     *                 a null template or there's no type.
     * @param typeDesc might be null in case the space didn't introduce this type (might happen in
     *                 case of inheritance)
     */
    public SQLQuery<?> build(ITemplateHolder template, String typeName, ITypeDesc typeDesc) {
        // Handle unknown types 
        if (typeDesc == null)
            return new SQLQuery<Object>(typeName, "");

        return template.toSQLQuery(typeDesc);


    }

    /**
     * @return Object that was converted to SQL format
     */
    public static Object convertToSQLFormat(Object object, short matchCode) {
        if (matchCode == TemplateMatchCodes.REGEX)
            return object.toString().replaceAll("(\\.\\*)", "%").replaceAll(
                    "\\.", "_");
        return object;
    }

    /* (non-Javadoc)
   * @see com.j_spaces.sadapter.datasource.SQLQueryBuilder#build(com.j_spaces.core.EntryHolder, com.j_spaces.core.server.TypeTableEntry)
   */
    public SQLQuery<?> build(IEntryPacket entry, ITypeDesc typeDesc) {
        List<Object> preparedValues = new LinkedList<Object>();

        List<String> idPropertiesNames = typeDesc.getIdPropertiesNames();
        if (idPropertiesNames.isEmpty() && typeDesc.getDefaultPropertyName() != null)
            idPropertiesNames = Collections.singletonList(typeDesc.getDefaultPropertyName());

        //if no fields are defined - create an empty query
        if (idPropertiesNames.isEmpty())
            return new SQLQuery<Object>(entry.getTypeName(), "");

        // Add the field values to the prepared values
        if (!typeDesc.getIdPropertiesNames().isEmpty() && typeDesc.isAutoGenerateId()) {
            preparedValues.add(entry.getUID());
        } else {
            for (String name : idPropertiesNames) {
                preparedValues.add(entry.getPropertyValue(name));
            }
        }

        StringBuilder wherePart = new StringBuilder();
        appendEqualsPredicate(wherePart, idPropertiesNames.get(0), 0);
        for (int i = 1; i < idPropertiesNames.size(); i++) {
            wherePart.append(" AND ");
            appendEqualsPredicate(wherePart, idPropertiesNames.get(i), i);
        }

        SQLQuery<?> query = new SQLQuery<Object>(entry.getTypeName(), wherePart.toString());
        query.setParameters(preparedValues.toArray());
        return query;
    }

    private void appendEqualsPredicate(StringBuilder wherePart, String name, int pos) {
        wherePart.append(name);
        wherePart.append(mapCodeToSign(TemplateMatchCodes.EQ));
        wherePart.append(BIND_PARAMETER);
        if (ADAPT_POSITIONAL_PARAMETERS)
            wherePart.append(pos);
    }

    /**
     * Maps between extended match-codes to actual SQL Query sign.
     *
     * @param code the extended match-code to map
     * @return the corresponding SQL Query sign (according to extendedMatchCodeMapping)
     */
    public static String mapCodeToSign(short code) {
        return extendedMatchCodeMapping[code];
    }


}
