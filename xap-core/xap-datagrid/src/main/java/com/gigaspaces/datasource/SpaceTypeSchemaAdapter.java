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
package com.gigaspaces.datasource;

import com.gigaspaces.document.SpaceDocument;
import com.gigaspaces.metadata.SpaceTypeDescriptor;

import java.io.Serializable;

/**
 * @author Alon Shoham
 * @since 15.5.0
 */
public interface SpaceTypeSchemaAdapter extends Serializable {
    SpaceDocument adaptEntry(SpaceDocument spaceDocument);

    SpaceTypeDescriptor adaptTypeDescriptor(SpaceTypeDescriptor spaceTypeDescriptor);

    String getTypeName();
}
