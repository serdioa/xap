package com.gigaspaces.utils;

import com.gigaspaces.logger.Constants;
import com.gigaspaces.start.SystemLocations;
import com.j_spaces.kernel.ClassLoaderHelper;
import com.j_spaces.kernel.SystemProperties;
import com.sun.jini.system.FileSystem;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.file.*;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.EnumSet;

public class FileUtils {

    private static final Logger _logger = LoggerFactory.getLogger(Constants.LOGGER_REPLICATION_NODE);

    /*
    Copy files from src folder to dest folder, If filter is not null will copy only files that their name contain the filter
    Based on FileSystemUtils.copyRecursively
     */
    public static void copyRecursively(final Path src, final Path dest, String filter) throws IOException {
        if (src == null || dest == null)
            throw new IllegalArgumentException("src And dest of copy folders can't be null");
        BasicFileAttributes srcAttr = Files.readAttributes(src, BasicFileAttributes.class);
        if (srcAttr.isDirectory()) {
            Files.walkFileTree(src, EnumSet.of(FileVisitOption.FOLLOW_LINKS), Integer.MAX_VALUE, new SimpleFileVisitor<Path>() {
                public FileVisitResult preVisitDirectory(Path dir, BasicFileAttributes attrs) throws IOException {
                    Files.createDirectories(dest.resolve(src.relativize(dir)));
                    return FileVisitResult.CONTINUE;
                }

                public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
                    if (filter != null && !file.toFile().getName().contains(filter))
                        return FileVisitResult.CONTINUE;
                    Files.copy(file, dest.resolve(src.relativize(file)), StandardCopyOption.REPLACE_EXISTING);
                    return FileVisitResult.CONTINUE;
                }
            });
        } else {
            if (!srcAttr.isRegularFile()) {
                throw new IllegalArgumentException("Source File must denote a directory or file");
            }
            if (filter!=null && !src.toFile().getName().contains(filter));
            else {
                Files.copy(src, dest);
            }
        }

    }

    public static void copyRecursively(final Path src, final Path dest) throws IOException{
        copyRecursively(src,dest, null);
    }

    public static void notifyOnFlushRedologToStorage(String fullSpaceName, String space, long redologSize, Path target){
        String className = System.getProperty(SystemProperties.REDOLOG_FLUSH_NOTIFY_CLASS, null);
        if (className == null){
            _logger.info(SystemProperties.REDOLOG_FLUSH_NOTIFY_CLASS + " not set - no notification is called");
            return;
        }
        try {
            Class<RedologFlushNotifier> loadClass = ClassLoaderHelper.loadClass(className, true);
            RedologFlushNotifier notifier = loadClass.newInstance();
            notifier.notifyOnFlush(fullSpaceName, space, redologSize, target);
            _logger.info("notifier.notifyOnFlush was called. class=" + className);
        } catch (Exception e) {
            _logger.error("Calling specified " + SystemProperties.REDOLOG_FLUSH_NOTIFY_CLASS + " [" + className + "] failed", e);
        }
    }

    public static Path copyRedologToTarget(String spaceName, String fullSpaceName) throws IOException{
        String filter = fullSpaceName.substring(0, fullSpaceName.indexOf(":"));
        Path directoryTarget = SystemLocations.singleton().work("redo-log-backup").resolve(spaceName);
        Path directorySrc = SystemLocations.singleton().work("redo-log").resolve(spaceName);
        if (!directoryTarget.toFile().exists()) directoryTarget.toFile().mkdirs();

        copyRecursively(directorySrc, directoryTarget,filter);
        return directoryTarget;
    }


}
