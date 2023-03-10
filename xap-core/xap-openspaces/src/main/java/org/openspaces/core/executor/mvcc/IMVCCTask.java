package org.openspaces.core.executor.mvcc;

import org.openspaces.core.executor.Task;

/**
 * @author Sagiv Michael
 * @since 16.3.0
 */
public interface IMVCCTask<T extends IMVCCTaskResult> extends Task<T> {

    long getActiveGeneration();

}
