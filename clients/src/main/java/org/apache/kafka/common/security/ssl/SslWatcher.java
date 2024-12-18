/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.kafka.common.security.ssl;

import org.apache.kafka.common.utils.KafkaThread;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.ClosedWatchServiceException;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardWatchEventKinds;
import java.nio.file.WatchEvent;
import java.nio.file.WatchKey;
import java.nio.file.WatchService;
import java.time.Duration;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/**
 * Watches files and directories and triggers a callback on change.
 *
 */
class SslWatcher {

    private static final Logger log = LoggerFactory.getLogger(SslWatcher.class);

    // the duration that no file changes should occur before reconfiguring
    // this is not configurable to avoid having different values for the
    // watchService's poll
    private static Duration quietPeriod = Duration.ofSeconds(30);

    private static final Object LOCK = new Object();

    private static WatcherThread thread;

    private static SslWatcher instance;

    private SslWatcher() {
    }

    // VisibleForTesting
    static void setQuietPeriod(Duration duration) {
        SslWatcher.quietPeriod = duration;
    }

    /**
     * Returns the singleton instance of {@code SslWatcher}.
     * Ensures thread-safety by using synchronized initialization.
     * 
     * @return the singleton instance of {@code SslWatcher}
     */
    public static synchronized SslWatcher getInstance() {
        if (instance == null) {
            instance = new SslWatcher();
        }
        return instance;
    }

    /**
     * Watch the given files or directories for changes.
	 * <p>The specified {@code action} will be executed after a change is detected, 
	 * but only after a "quiet period" of at least 30 seconds has elapsed since the last change. 
 	 * This helps avoid triggering the action for multiple rapid, successive changes.</p>
     * 
     * @param paths  the files or directories to watch
     * @param action the action to take when changes are detected
     */
    Registration watch(Set<Path> paths, Runnable action) {
        if (paths == null || action == null) {
            throw new IllegalStateException("Paths and action must not be null.");
        }
        if (paths.isEmpty()) {
            return null;
        }
        Registration registration = null;
        synchronized (SslWatcher.LOCK) {
            try {
                if (SslWatcher.thread == null) {
                    SslWatcher.thread = new WatcherThread();
                    SslWatcher.thread.start();
                }
                registration = new Registration(paths, action);
                SslWatcher.thread.register(registration);
                return registration;

            } catch (IOException ex) {
                throw new UncheckedIOException("Failed to register paths for watching: " + paths, ex);
            }
        }
    }

    /**
     * Closes the given {@link Registration} and stops the SSL watcher thread 
     * if there are no remaining registrations. The method synchronizes access 
     * to the thread, interrupts it, and waits for its termination before setting 
     * the thread reference to {@code null}.
     *
     * @param registration the {@link Registration} to be closed. The thread is stopped 
     *                     only if there are no remaining registrations.
     * @throws IOException if an I/O error occurs during the close operation.
     * @throws InterruptedException if interrupted while waiting for the thread to terminate.
     */
    public static void close(Registration registration) throws IOException {
        if (instance != null) {
            synchronized (SslWatcher.LOCK) {
                if (SslWatcher.thread != null) {
                    boolean stopped = SslWatcher.thread.close(registration);
                    if (stopped) {

                        SslWatcher.thread.interrupt();
                        try {
                            SslWatcher.thread.join();
                        } catch (InterruptedException ex) {
                            Thread.currentThread().interrupt();
                        }
                        SslWatcher.thread = null;
                    }
                }
            }
        }

    }

    private static class WatcherThread extends KafkaThread {

        private final WatchService watchService = FileSystems.getDefault().newWatchService();

        private final Set<Registration> registrations = new HashSet<>();

        private volatile boolean running = true;

        public WatcherThread() throws IOException {
            // Daemon KafkaThread
            super("ssl-watcher", true);
        }

        void register(Registration registration) throws IOException {
            for (Path path : registration.getPaths()) {
                if (!Files.isRegularFile(path) && !Files.isDirectory(path)) {
                    throw new IOException(String.format("'%s' is neither a file nor a directory", path));
                }
                Path directory = Files.isDirectory(path) ? path : path.getParent();
                if (directory != null) {
                    log.debug("Registering '%s'", directory);
                    directory.register(this.watchService, StandardWatchEventKinds.ENTRY_CREATE,
                        StandardWatchEventKinds.ENTRY_MODIFY, StandardWatchEventKinds.ENTRY_DELETE);
                }
            }
            this.registrations.add(registration);
        }

        @Override
        public void run() {
            log.debug("Watch thread started");
            Set<Runnable> actions = new HashSet<>();
            while (this.running) {
                try {
                    long timeout = SslWatcher.quietPeriod.toMillis();
                    WatchKey key = this.watchService.poll(timeout, TimeUnit.MILLISECONDS);
                    if (key == null) {
                        actions.forEach(this::runSafely);
                        actions.clear();
                    } else {
                        accumulate(key, actions);
                    }
                } catch (InterruptedException ex) {
                    Thread.currentThread().interrupt();
                } catch (ClosedWatchServiceException ex) {
                    log.debug("SslWatcher has been closed");
                    this.running = false;
                }
            }
            log.debug("SslWatcher thread stopped");
        }

        private void runSafely(Runnable action) {
            try {
                action.run();
            } catch (Throwable ex) {
                log.error("Unexpected SSL reconfigure error", ex);
            }
        }

        private void accumulate(WatchKey key, Set<Runnable> actions) {
            boolean resetKey = false;
            Path directory = (Path) key.watchable();
            for (WatchEvent<?> event : key.pollEvents()) {
                Path file = directory.resolve((Path) event.context());
                for (Registration registration : this.registrations) {
                    if (registration.getPaths().contains(file.toAbsolutePath())) {
                        actions.add(registration.getAction());
                        resetKey = true;
                    }
                }
            }
            if (resetKey)
                key.reset();
        }

        /**
         * Closes the registration and shuts down the watch service and stops the thread if no more registrations remain.
         *
         * @param registration the {@link Registration} to be removed and closed
         * @return {@code true} if the watch service was stopped (i.e., no registrations left), 
         *         {@code false} otherwise
         * @throws IOException if an I/O error occurs while closing the watch service
         */
        public boolean close(Registration registration) throws IOException {
            boolean stopped = false;

            this.registrations.remove(registration);
            if (this.registrations.isEmpty()) {
                this.running = false;
                this.watchService.close();
                stopped = true;
            }
            return stopped;
        }

    }

    public static class Registration {

        private final Set<Path> paths;
        private final Runnable action;

        public Registration(Set<Path> paths, Runnable action) {
            this.paths = paths.stream()
                    .map(Path::toAbsolutePath)
                    .collect(Collectors.toSet());
            this.action = action;
        }

        public Set<Path> getPaths() {
            return paths;
        }

        public Runnable getAction() {
            return action;
        }
    }

}
