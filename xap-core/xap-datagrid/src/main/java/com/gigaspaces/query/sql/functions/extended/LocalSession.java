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
package com.gigaspaces.query.sql.functions.extended;

import com.gigaspaces.client.IsolationLevelModifiers;
import com.gigaspaces.client.TransactionModeModifiers;
import net.jini.core.transaction.Transaction;

import java.io.Serializable;

public class LocalSession implements Serializable {

    private static final long serialVersionUID = -2470679645214359948L;
    private String username;
    private int gwPort;
    private String sessionId;
    private Transaction transaction;
    private IsolationLevelModifiers isolationLevelModifiers;
    private TransactionModeModifiers transactionMode;

    public LocalSession() {
    }

    public LocalSession(String username) {
        this.username = username;
    }

    public LocalSession(String username, int gwPort) {
        this.username = username;
        this.gwPort = gwPort;
    }

    public String getSessionId() {
        return sessionId;
    }

    public void setSessionId(String sessionId) {
        this.sessionId = sessionId;
    }

    public String getUsername() {
        return username;
    }

    public void setUsername(String username) {
        this.username = username;
    }

    public int getGwPort() {
        return gwPort;
    }

    public void setGwPort(int gwPort) {
        this.gwPort = gwPort;
    }

    public Transaction getTransaction() {
        return transaction;
    }

    public void setTransaction(Transaction transaction) {
        this.transaction = transaction;
    }

    public IsolationLevelModifiers getIsolationLevelModifiers() {
        return isolationLevelModifiers;
    }

    public void setIsolationLevelModifiers(IsolationLevelModifiers isolationLevelModifiers) {
        this.isolationLevelModifiers = isolationLevelModifiers;
    }

    public TransactionModeModifiers getTransactionMode() {
        return transactionMode;
    }

    public void setTransactionMode(TransactionModeModifiers transactionMode) {
        this.transactionMode = transactionMode;
    }
}
