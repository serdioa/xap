package org.openspaces.core.executor.mvcc.protocol;

import com.gigaspaces.internal.client.QueryResultTypeInternal;
import com.gigaspaces.internal.client.spaceproxy.IDirectSpaceProxy;
import com.gigaspaces.internal.server.space.SpaceImpl;
import com.gigaspaces.internal.server.space.mvcc.MVCCGenerationsState;
import com.j_spaces.core.client.TakeModifiers;
import net.jini.core.lease.LeaseDeniedException;
import net.jini.core.transaction.Transaction;
import net.jini.core.transaction.TransactionFactory;
import net.jini.core.transaction.server.TransactionManager;
import org.openspaces.core.GigaSpace;
import org.openspaces.core.executor.TaskGigaSpace;
import org.openspaces.core.executor.mvcc.IMVCCTask;
import org.openspaces.core.executor.mvcc.exception.MVCCRevertGenerationTaskException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.rmi.RemoteException;
import java.time.Duration;
import java.util.Arrays;

public class MVCCRevertGenerationTask implements IMVCCTask<MVCCRevertGenerationTaskResult> {
    private static final long serialVersionUID = 381666345179297373L;
    private static final Logger logger =
            LoggerFactory.getLogger("org.openspaces.core.executor.mvcc.task.revert-generation");
    public static final long DEFAULT_TRANSACTION_LEASE = Duration.ofMinutes(5).toMillis();
    private final long generation;
    private final Object[][] ids;
    private final Class<?>[] types;
    @TaskGigaSpace
    private transient GigaSpace space;


    public MVCCRevertGenerationTask(long generation, Object[][] ids, Class<?>[] types) {
        this.generation = generation;
        this.ids = ids;
        this.types = types;
        if (ids.length != types.length) {
            throw new IllegalArgumentException("Types length [" + types.length + "] not equals to ids [" + ids.length + "]");
        }
    }

    @Override
    public MVCCRevertGenerationTaskResult execute() throws Exception {
        final long partitionId = getEmbeddedSpaceImpl().getPartitionIdOneBased();
        final long startTime = System.currentTimeMillis();
        getDirectProxy().setMVCCGenerationsState(MVCCGenerationsState.revertGenerationState(generation));
        MVCCRevertGenerationTaskResult result = new MVCCRevertGenerationTaskResult();
        Transaction embeddedTransaction = createEmbeddedTransaction();
        try {
            for (int i = 0; i < types.length; i++) {
                Class<?> typeToRevert = types[i];
                Object[] idsToRevert = ids[i];
                Object[] revertedIds = getDirectProxy().takeByIds(typeToRevert.getName(), idsToRevert, null, embeddedTransaction,
                        TakeModifiers.MVCC_REVERT_GENERATION, QueryResultTypeInternal.NOT_SET, false, null);
                if (logger.isDebugEnabled()) {
                    logger.debug("Revert task for partition [" + partitionId + "],\n" +
                            " ids to revert - " + Arrays.toString(idsToRevert) + ",\n" +
                            " type to revert - " + typeToRevert + ",\n" +
                            " generation to revert - " + generation + ",\n" +
                            " reverted entries - " + Arrays.toString(revertedIds));
                }
                for (int index = 0; revertedIds != null && index < revertedIds.length; index++) {
                    if (revertedIds[index] == null) {
                        result.setException(new MVCCRevertGenerationTaskException("Failed to revert id [" + idsToRevert[index] + "]" +
                                " for type [" + typeToRevert.getName() + "]" +
                                " in partition [" + partitionId + "]"));
                        break;
                    }
                }
            }
            if (result.getException() == null) {
                if (logger.isDebugEnabled()) {
                    logger.debug("Revert task for partition [" + partitionId + "], succeed");
                }
                embeddedTransaction.commit();
            } else {
                if (logger.isDebugEnabled()) {
                    logger.debug("Revert task for partition [" + partitionId + "], failed");
                }
                embeddedTransaction.abort();
            }
        } catch (Throwable t) {
            result.setException(new MVCCRevertGenerationTaskException("Failed to revert generation [" + generation + "]" +
                    " in partition [" + partitionId + "]", t));
            if (logger.isDebugEnabled()) {
                logger.debug("Revert task for partition [" + partitionId + "], failed", t);
            }
            embeddedTransaction.abort();
        }
        result.setExecutionTime(System.currentTimeMillis() - startTime);
        clearGenerationState();
        return result;
        //TODO: check it uncompleted Gen otherwise EXP.
    }

    private Transaction createEmbeddedTransaction() throws LeaseDeniedException, RemoteException {
        final SpaceImpl spaceImpl = getEmbeddedSpaceImpl();
        final TransactionManager embeddedTransactionManager = spaceImpl.getContainer().getEmbeddedTransactionManager();
        return TransactionFactory.create(embeddedTransactionManager, DEFAULT_TRANSACTION_LEASE).transaction;
    }

    private SpaceImpl getEmbeddedSpaceImpl() {
        return getDirectProxy().getSpaceImplIfEmbedded();
    }

    private IDirectSpaceProxy getDirectProxy() {
        return space.getSpace().getDirectProxy();
    }

    private void clearGenerationState() {
        space.getSpace().getDirectProxy().setMVCCGenerationsState(null);
    }

    @Override
    public long getActiveGeneration() {
        return -1;
    }
}
