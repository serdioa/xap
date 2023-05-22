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
package org.openspaces.core.executor.mvcc.exception;

/**
 * @author Sagiv Michael
 * @since 16.3.0
 */
public class MVCCRetryTaskException extends Exception {
    private static final long serialVersionUID = 7382997159809192328L;

    public MVCCRetryTaskException() {
    }

    public MVCCRetryTaskException(String message) {
        super(message);
    }

    public MVCCRetryTaskException(String message, Throwable cause) {
        super(message, cause);
    }
}
