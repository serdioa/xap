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
package com.j_spaces.core.cache.mvcc;

import com.gigaspaces.internal.server.metadata.IServerTypeDesc;
import com.j_spaces.core.cache.CacheManager;
import com.j_spaces.core.cache.TypeData;
import com.j_spaces.core.sadapter.ISAdapterIterator;
import com.j_spaces.core.sadapter.SAException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Iterator;

/**
 * Iterator for mvcc shells
 *
 * @author Davyd Savitskyi
 * @since 16.4.0
 */
@com.gigaspaces.api.InternalApi
public class MVCCShellsRecoveryIter implements ISAdapterIterator<MVCCShellEntryCacheInfo> {

    private static Logger _logger = LoggerFactory.getLogger(MVCCShellsRecoveryIter.class.getName());

    private final CacheManager _cacheManager;
    private final IServerTypeDesc[] _types;
    private Iterator<MVCCShellEntryCacheInfo> _shellsIter;
    private int currentClass = 0;

    public MVCCShellsRecoveryIter(IServerTypeDesc serverTypeDesc, CacheManager cacheManager) {
        _cacheManager = cacheManager;
        _types = serverTypeDesc.getAssignableTypes();
    }

    /**
     * Iterates through shells list from types[] array.
     * Returns null after last was retrieved (after last shell for last type)
     * */
    @Override
    public MVCCShellEntryCacheInfo next() throws SAException {
        for (; currentClass < _types.length; currentClass++) {
            if (_shellsIter == null) {
                TypeData typeData = _cacheManager.getTypeData(_types[currentClass]);
                if (typeData == null || typeData.getIdField() == null) {
                    _logger.debug("Unable to retrieve entries with type {}", _types[currentClass]);
                    continue;
                }
                _shellsIter = typeData.getIdField().getUniqueEntriesStore().values().iterator();
            }
            if (_shellsIter.hasNext()) {
                return _shellsIter.next();
            }
            _shellsIter = null;
        }
        return null;
    }

    @Override
    public void close() throws SAException {
        _shellsIter = null;
        currentClass = 0;
    }
}
