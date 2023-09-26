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
package org.openspaces.test.persistency.hibernate;

import com.gigaspaces.datasource.DataIterator;
import com.gigaspaces.datasource.DataSourceSQLQuery;
import com.gigaspaces.datasource.SQLQueryToDataSourceSQLQueryAdapter;
import com.j_spaces.core.client.SQLQuery;
import org.hibernate.*;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.openspaces.persistency.hibernate.iterator.DefaultListQueryDataIterator;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class DefaultListQueryDataIteratorTests {

    private SessionFactory sessionFactory;
    private Session session;

    @Before
    public void beforeTest() {
        sessionFactory = mock(SessionFactory.class);
        session = mock(Session.class);
        when(sessionFactory.openSession()).thenReturn(session);
    }

    @Test
    public void iteratorWithTypeName() {
        String typeName = "foo";
        Criteria criteria = mock(Criteria.class);
        when(session.createCriteria(typeName)).thenReturn(criteria);
        when(criteria.list()).thenThrow(HibernateException.class);

        DataIterator iterator = new DefaultListQueryDataIterator(typeName, sessionFactory);
        try {
            iterator.hasNext();
            Assert.fail("Should have failed");
        } catch (Exception e) {
            System.out.println("Intercepted expected exception: " + e.getMessage());
            if (!e.getMessage().contains(typeName))
                Assert.fail("Exception message does not contain type name: " + e.getMessage());
        }
    }

    @Test
    public void iteratorWithSqlQuery() {
        String typeName = "foo";
        SQLQuery sqlQuery = new SQLQuery(typeName, "bar = 1");

        Query query = mock(Query.class);
        when(session.createQuery(sqlQuery.getFromQuery())).thenReturn(query);
        when(query.list()).thenThrow(HibernateException.class);

        DataIterator iterator = new DefaultListQueryDataIterator(sqlQuery, sessionFactory);
        try {
            iterator.hasNext();
            Assert.fail("Should have failed");
        } catch (Exception e) {
            System.out.println("Intercepted expected exception: " + e.getMessage());
            if (!e.getMessage().contains(typeName))
                Assert.fail("Exception message does not contain type name: " + e.getMessage());
        }
    }

    @Test
    public void iteratorWithDataSourceQuery() {
        String typeName = "foo";
        SQLQuery sqlQuery = new SQLQuery(typeName, "bar = 2");
        DataSourceSQLQuery dsQuery = new SQLQueryToDataSourceSQLQueryAdapter(sqlQuery);

        Query query = mock(Query.class);
        when(session.createQuery(sqlQuery.getFromQuery())).thenReturn(query);
        when(query.list()).thenThrow(HibernateException.class);

        DataIterator iterator = new DefaultListQueryDataIterator(dsQuery, sessionFactory);
        try {
            iterator.hasNext();
            Assert.fail("Should have failed");
        } catch (Exception e) {
            System.out.println("Intercepted expected exception: " + e.getMessage());
            if (!e.getMessage().contains(typeName))
                Assert.fail("Exception message does not contain type name: " + e.getMessage());
        }
    }
}
