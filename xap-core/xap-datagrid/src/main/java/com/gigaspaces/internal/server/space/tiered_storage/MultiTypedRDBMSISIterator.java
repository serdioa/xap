package com.gigaspaces.internal.server.space.tiered_storage;

import com.gigaspaces.internal.server.metadata.IServerTypeDesc;
import com.gigaspaces.internal.server.storage.IEntryHolder;
import com.gigaspaces.internal.server.storage.ITemplateHolder;
import com.j_spaces.core.sadapter.ISAdapterIterator;
import com.j_spaces.core.sadapter.SAException;

public class MultiTypedRDBMSISIterator implements ISAdapterIterator<IEntryHolder> {

    private final TieredStorageSA tieredStorageSA;
    private final IServerTypeDesc[] types;
    private final ITemplateHolder templateHolder;
    private ISAdapterIterator<IEntryHolder> currentTypeIterator;
    private int currentTypeIndex;
    private boolean finished;

    public MultiTypedRDBMSISIterator(TieredStorageSA tieredStorageSA, IServerTypeDesc[] types, ITemplateHolder templateHolder) {
        this.tieredStorageSA = tieredStorageSA;
        this.types = types;
        this.templateHolder = templateHolder;
        this.currentTypeIndex = 0;
    }

    @Override
    public IEntryHolder next() throws SAException {
        if (finished) {
            return null;
        }

        if (currentTypeIterator == null) {
            currentTypeIterator = createNextTypeIterator();
            if (currentTypeIterator == null) {
                finished = true;
                return null;
            }
        }

        IEntryHolder next = currentTypeIterator.next();
        if (next == null) {
            if (currentTypeIndex == types.length - 1) {
                finished = true;
                return null;
            } else {
                currentTypeIterator = null;
                currentTypeIndex++;
                return next();
            }
        } else {
            return next;
        }
    }

    private ISAdapterIterator<IEntryHolder> createNextTypeIterator() throws SAException {
        boolean foundType = false;
        while (!foundType && currentTypeIndex < types.length) {
            foundType = tieredStorageSA.isKnownType(types[currentTypeIndex].getTypeName());
            if (!foundType) {
                currentTypeIndex++;
            }
        }

        if (foundType) {
            return tieredStorageSA.makeSingleTypeEntriesIter(types[currentTypeIndex].getTypeName(), templateHolder);
        } else {
            return null;
        }
    }

    @Override
    public void close() throws SAException {
        if (currentTypeIterator != null) {
            currentTypeIterator.close();
        }
    }
}
