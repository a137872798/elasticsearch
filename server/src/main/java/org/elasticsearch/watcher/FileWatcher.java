/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.elasticsearch.watcher;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.common.io.FileSystemUtils;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.Arrays;

/**
 * File resources watcher
 *
 * The file watcher checks directory and all its subdirectories for file changes and notifies its listeners accordingly
 * 这里只是定义了监控触发时 执行的逻辑  实际上这是一个定期检测文件是否还存活的对象
 */
public class FileWatcher extends AbstractResourceWatcher<FileChangesListener> {

    /**
     * 整个文件系统是一个树结构 当前Observer代表顶层节点
     */
    private FileObserver rootFileObserver;

    /**
     * 监控的文件/目录
     */
    private Path file;

    private static final Logger logger = LogManager.getLogger(FileWatcher.class);

    /**
     * Creates new file watcher on the given directory
     * 在初始化时 指定监控的目录
     */
    public FileWatcher(Path file) {
        this.file = file;
        rootFileObserver = new FileObserver(file);
    }

    /**
     * Clears any state with the FileWatcher, making all files show up as new
     */
    public void clearState() {
        rootFileObserver = new FileObserver(file);
        try {
            rootFileObserver.init(false);
        } catch (IOException e) {
            // ignore IOException
        }
    }

    @Override
    protected void doInit() throws IOException {
        rootFileObserver.init(true);
    }

    @Override
    protected void doCheckAndNotify() throws IOException {
        rootFileObserver.checkAndNotify();
    }

    private static FileObserver[] EMPTY_DIRECTORY = new FileObserver[0];

    /**
     * 生成一个文件监控对象
     */
    private class FileObserver {
        private Path file;
        private boolean exists;
        private long length;
        private long lastModified;
        private boolean isDirectory;
        private FileObserver[] children;

        FileObserver(Path file) {
            this.file = file;
        }

        /**
         * 资源监控服务 每隔监控器会在一定的时期触发所有设置的监控对象的 checkAndNotify
         * @throws IOException
         */
        public void checkAndNotify() throws IOException {
            boolean prevExists = exists;
            boolean prevIsDirectory = isDirectory;
            long prevLength = length;
            long prevLastModified = lastModified;

            exists = Files.exists(file);
            // TODO we might use the new NIO2 API to get real notification?
            if (exists) {
                BasicFileAttributes attributes = Files.readAttributes(file, BasicFileAttributes.class);
                isDirectory = attributes.isDirectory();
                if (isDirectory) {
                    length = 0;
                    lastModified = 0;
                } else {
                    length = attributes.size();
                    lastModified = attributes.lastModifiedTime().toMillis();
                }
            // 因为此时file 已经不存在了 重置相关参数
            } else {
                isDirectory = false;
                length = 0;
                lastModified = 0;
            }

            // Perform notifications and update children for the current file
            // 代表之前文件是存在的
            if (prevExists) {
                if (exists) {
                    // 代表此时文件/目录还是存在的 尝试更新信息
                    if (isDirectory) {
                        if (prevIsDirectory) {
                            // Remained a directory
                            updateChildren();
                        } else {
                            // File replaced by directory
                            onFileDeleted();
                            onDirectoryCreated(false);
                        }
                    } else {
                        if (prevIsDirectory) {
                            // Directory replaced by file
                            onDirectoryDeleted();
                            onFileCreated(false);
                        } else {
                            // Remained file
                            if (prevLastModified != lastModified || prevLength != length) {
                                onFileChanged();
                            }
                        }
                    }
                // 代表当前目录已经被删除了 触发相关钩子
                } else {
                    // Deleted
                    if (prevIsDirectory) {
                        onDirectoryDeleted();
                    } else {
                        onFileDeleted();
                    }
                }
            // 代表此时新建了文件/目录
            } else {
                // Created
                if (exists) {
                    if (isDirectory) {
                        onDirectoryCreated(false);
                    } else {
                        onFileCreated(false);
                    }
                }
            }

        }

        /**
         * 构建整个树结构
         * @param initial
         * @throws IOException
         */
        private void init(boolean initial) throws IOException {
            exists = Files.exists(file);
            if (exists) {
                BasicFileAttributes attributes = Files.readAttributes(file, BasicFileAttributes.class);
                // 读取文件的相关属性并设置
                isDirectory = attributes.isDirectory();
                if (isDirectory) {
                    onDirectoryCreated(initial);
                } else {
                    // 代表file指代的是一个文件
                    length = attributes.size();
                    lastModified = attributes.lastModifiedTime().toMillis();
                    onFileCreated(initial);
                }
            }
        }

        private FileObserver createChild(Path file, boolean initial) throws IOException {
            FileObserver child = new FileObserver(file);
            child.init(initial);
            return child;
        }

        private Path[] listFiles() throws IOException {
            final Path[] files = FileSystemUtils.files(file);
            Arrays.sort(files);
            return files;
        }

        /**
         * 生成子级对象 因为文件系统本身是一个树结构
         * @param initial
         * @return
         * @throws IOException
         */
        private FileObserver[] listChildren(boolean initial) throws IOException {
            Path[] files = listFiles();
            if (files != null && files.length > 0) {
                FileObserver[] children = new FileObserver[files.length];
                for (int i = 0; i < files.length; i++) {
                    children[i] = createChild(files[i], initial);
                }
                return children;
            } else {
                return EMPTY_DIRECTORY;
            }
        }

        /**
         * 更新文件树信息
         * @throws IOException
         */
        private void updateChildren() throws IOException {
            Path[] files = listFiles();
            if (files != null && files.length > 0) {
                FileObserver[] newChildren = new FileObserver[files.length];
                int child = 0;
                int file = 0;
                while (file < files.length || child < children.length ) {
                    int compare;

                    if (file >= files.length) {
                        compare = -1;
                    } else if (child >= children.length) {
                        compare = 1;
                    } else {
                        compare = children[child].file.compareTo(files[file]);
                    }

                    if (compare  == 0) {
                        // Same file copy it and update
                        children[child].checkAndNotify();
                        newChildren[file] = children[child];
                        file++;
                        child++;
                    } else {
                        if (compare > 0) {
                            // This child doesn't appear in the old list - init it
                            newChildren[file] = createChild(files[file], false);
                            file++;
                        } else {
                            // The child from the old list is missing in the new list
                            // Delete it
                            deleteChild(child);
                            child++;
                        }
                    }
                }
                children = newChildren;
            } else {
                // No files - delete all children
                for (int child = 0; child < children.length; child++) {
                    deleteChild(child);
                }
                children = EMPTY_DIRECTORY;
            }
        }

        private void deleteChild(int child) {
            if (children[child].exists) {
                if (children[child].isDirectory) {
                    children[child].onDirectoryDeleted();
                } else {
                    children[child].onFileDeleted();
                }
            }
        }

        private void onFileCreated(boolean initial) {
            for (FileChangesListener listener : listeners()) {
                try {
                    if (initial) {
                        listener.onFileInit(file);
                    } else {
                        listener.onFileCreated(file);
                    }
                } catch (Exception e) {
                    logger.warn("cannot notify file changes listener", e);
                }
            }
        }

        private void onFileDeleted() {
            for (FileChangesListener listener : listeners()) {
                try {
                    listener.onFileDeleted(file);
                } catch (Exception e) {
                    logger.warn("cannot notify file changes listener", e);
                }
            }
        }

        private void onFileChanged() {
            for (FileChangesListener listener : listeners()) {
                try {
                    listener.onFileChanged(file);
                } catch (Exception e) {
                    logger.warn("cannot notify file changes listener", e);
                }

            }
        }

        /**
         * 代表当前设置的file是一个目录 那么要还要对下级所有文件/目录做处理
         * @param initial
         * @throws IOException
         */
        private void onDirectoryCreated(boolean initial) throws IOException {
            for (FileChangesListener listener : listeners()) {
                try {
                    if (initial) {
                        listener.onDirectoryInit(file);
                    } else {
                        // 代表文件才被创建
                        listener.onDirectoryCreated(file);
                    }
                } catch (Exception e) {
                    logger.warn("cannot notify file changes listener", e);
                }
            }
            children = listChildren(initial);
        }

        private void onDirectoryDeleted() {
            // First delete all children
            for (int child = 0; child < children.length; child++) {
                deleteChild(child);
            }
            for (FileChangesListener listener : listeners()) {
                try {
                    listener.onDirectoryDeleted(file);
                } catch (Exception e) {
                    logger.warn("cannot notify file changes listener", e);
                }
            }
        }

    }

}
