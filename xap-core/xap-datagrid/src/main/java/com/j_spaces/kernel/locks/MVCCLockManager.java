package com.j_spaces.kernel.locks;

import com.gigaspaces.internal.server.space.SpaceConfigReader;

import java.util.Arrays;

import static com.j_spaces.core.Constants.Mvcc.CACHE_MANAGER_MVCC_LOCKS_SIZE_DEFAULT;
import static com.j_spaces.core.Constants.Mvcc.CACHE_MANAGER_MVCC_LOCKS_SIZE_PROP;


@com.gigaspaces.api.InternalApi
public class MVCCLockManager<T extends ILockObject> implements IBasicLockManager<T> {
    private static class LockObject implements ILockObject {
        @Override
        public boolean isLockSubject() {
            return false;
        }

        @Override
        public String getUID() {
            return null;
        }
    }

    private final LockObject[] _locks;

    public MVCCLockManager(SpaceConfigReader configReader) {
        int size = configReader.getIntSpaceProperty(CACHE_MANAGER_MVCC_LOCKS_SIZE_PROP,
                CACHE_MANAGER_MVCC_LOCKS_SIZE_DEFAULT);
        _locks = new LockObject[size];
        Arrays.setAll(_locks, i -> new LockObject());
    }

    public MVCCLockManager() {
        int size = Integer.parseInt(CACHE_MANAGER_MVCC_LOCKS_SIZE_DEFAULT);
        _locks = new LockObject[size];
        Arrays.setAll(_locks, i -> new LockObject());
    }

    @Override
    public ILockObject getLockObject(T subject) {
        if (subject instanceof IMVCCLockObject) {
            return _locks[((IMVCCLockObject) subject).getLockedObjectHashCode() % _locks.length];
        } else {
            return subject;
        }
    }

    @Override
    public ILockObject getLockObject(String subjectUid) {
        throw new RuntimeException("MVCCLockManager::getLockObject based on uid is not supported");
    }

    @Override
    public void freeLockObject(ILockObject lockObject) {
    }

    @Override
    public boolean isEntryLocksItsSelf(T entry) {
        return false;
    }
}
