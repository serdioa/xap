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
package com.gigaspaces.internal.server.space.mvcc;

/**
 * @author Sagiv Michael
 * @since 16.3
 */
public class MVCCGenerationStateException extends RuntimeException {
    private static final long serialVersionUID = 913779341516565242L;

    public MVCCGenerationStateException() {
    }

    public MVCCGenerationStateException(String message) {
        super(message);
    }

    public MVCCGenerationStateException(String message, Throwable cause) {
        super(message, cause);
    }

    public MVCCGenerationStateException(Throwable cause) {
        super(cause);
    }
}
