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
package org.openspaces.core.executor.mvcc.protocol;

import org.openspaces.core.executor.mvcc.IMVCCTaskResult;

import java.util.HashSet;
import java.util.Set;

/**
 * @author Sagiv Michael
 * @since 16.3.0
 */
public abstract class AbstractMVCCProtocolTaskResult implements IMVCCTaskResult {
    private static final long serialVersionUID = -6559029718470261175L;
    private final Set<Long> activeGenerations = new HashSet<>();
    private long embeddedTransactionId;
    private Throwable exception;

    public Set<Long> getActiveGenerations() {
        return activeGenerations;
    }

    public void addActiveGeneration(long activeGeneration) {
        this.activeGenerations.add(activeGeneration);
    }

    public long getEmbeddedTransactionId() {
        return embeddedTransactionId;
    }

    public void setEmbeddedTransactionId(long embeddedTransactionId) {
        this.embeddedTransactionId = embeddedTransactionId;
    }

    public Throwable getException() {
        return exception;
    }

    public void setException(Throwable exception) {
        this.exception = exception;
    }
}
