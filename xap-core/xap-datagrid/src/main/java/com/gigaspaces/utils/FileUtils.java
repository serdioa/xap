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
package com.gigaspaces.utils;

import com.gigaspaces.start.SystemLocations;
import com.sun.jini.system.FileSystem;

import java.io.*;
import java.nio.file.*;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.EnumSet;

public class FileUtils {
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


}
