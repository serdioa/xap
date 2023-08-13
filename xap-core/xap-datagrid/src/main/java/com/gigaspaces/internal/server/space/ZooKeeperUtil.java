package com.gigaspaces.internal.server.space;

import com.gigaspaces.attribute_store.AttributeStore;
import com.j_spaces.core.admin.ZKCollocatedClientConfig;
import com.j_spaces.kernel.ClassLoaderHelper;
import com.j_spaces.kernel.log.JProperties;
import org.slf4j.Logger;

import java.lang.reflect.Constructor;
import java.util.Properties;

import static com.j_spaces.core.Constants.DirectPersistency.ZOOKEEPER.ATTRIBUET_STORE_HANDLER_CLASS_NAME;
import static com.j_spaces.core.Constants.DirectPersistency.ZOOKEEPER.ZOOKEEPER_CLIENT_CLASS_NAME;

public final class ZooKeeperUtil {

    public static AttributeStore createZooKeeperAttributeStore(ZKCollocatedClientConfig zkConfig, Logger logger,
                                                                String spaceName) {
        try {
            //noinspection unchecked
            Constructor constructor = ClassLoaderHelper.loadLocalClass(ATTRIBUET_STORE_HANDLER_CLASS_NAME)
                    .getConstructor(ZKCollocatedClientConfig.class);
            return (AttributeStore) constructor.newInstance(zkConfig);

        } catch (Exception e) {
            if (logger.isErrorEnabled())
                logger.error("Failed to create attribute store ",e);
            throw new RuntimeException("Failed to start [" + spaceName + "] Failed to create attribute store.");
        }
    }

    public static ZookeeperClient createZooKeeperClient(ZKCollocatedClientConfig zkConfig, Logger logger,
                                                         String spaceName) {
        try {
            //noinspection unchecked
            Constructor constructor = ClassLoaderHelper.loadLocalClass(ZOOKEEPER_CLIENT_CLASS_NAME)
                    .getConstructor(ZKCollocatedClientConfig.class);
            return (ZookeeperClient) constructor.newInstance(zkConfig);

        } catch (Exception e) {
            if (logger.isErrorEnabled())
                logger.error("Failed to create zookeeper client",e);
            throw new RuntimeException("Failed to start [" + (spaceName) + "] Failed to create zookeeper client.");
        }
    }

    public static ZKCollocatedClientConfig createZKCollocatedClientConfig(String fullSpaceName) {
        Properties props = JProperties.getSpaceProperties(fullSpaceName);
        return new ZKCollocatedClientConfig(props);
    }
}
