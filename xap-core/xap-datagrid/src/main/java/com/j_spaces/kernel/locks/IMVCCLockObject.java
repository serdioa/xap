package com.j_spaces.kernel.locks;

/**
 * The basic interface for a MVCC lock object.
 * include method to get the representing locked object hashCode that used in {@link MVCCLockManager}.
 * @author Sagiv Michael
 * @since 16.3.0
 */
public interface IMVCCLockObject extends ILockObject {

    int getLockedObjectHashCode();
}
