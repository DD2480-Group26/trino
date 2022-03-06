/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.trino.plugin.hive;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.MINUTES;
import static org.apache.commons.lang3.StringUtils.isNotEmpty;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import javax.inject.Inject;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.cache.Cache;
import com.google.common.cache.Weigher;
import com.google.common.collect.ImmutableList;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.weakref.jmx.Managed;

import io.airlift.units.Duration;
import io.trino.collect.cache.EvictableCacheBuilder;
import io.trino.plugin.hive.metastore.Partition;
import io.trino.plugin.hive.metastore.Storage;
import io.trino.plugin.hive.metastore.Table;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.connector.SchemaTablePrefix;

public class CachingDirectoryLister
        implements DirectoryLister, TableInvalidationCallback
{
	
    //TODO use a cache key based on Path & SchemaTableName and iterate over the cache keys
    // to deal more efficiently with cache invalidation scenarios for partitioned tables.
	private Duration fileListingTimeout = new Duration(1, MINUTES);
    private final Cache<Path, ValueHolder> cache;
    private final List<SchemaTablePrefix> tablePrefixes;


    @Inject
    public CachingDirectoryLister(HiveConfig hiveClientConfig)
    {
        this(hiveClientConfig.getFileStatusCacheExpireAfterWrite(), hiveClientConfig.getFileStatusCacheMaxSize(), hiveClientConfig.getFileStatusCacheTables());
    }

    public CachingDirectoryLister(Duration expireAfterWrite, long maxSize, List<String> tables)
    {
        this.cache = EvictableCacheBuilder.newBuilder()
                .maximumWeight(maxSize)
                .weigher((Weigher<Path, ValueHolder>) (key, value) -> value.files.map(List::size).orElse(1))
                .expireAfterWrite(expireAfterWrite.toMillis(), TimeUnit.MILLISECONDS)
                .shareNothingWhenDisabled()
                .recordStats()
                .build();
        this.tablePrefixes = tables.stream()
                .map(CachingDirectoryLister::parseTableName)
                .collect(toImmutableList());
    }

    private static SchemaTablePrefix parseTableName(String tableName)
    {
        if (tableName.equals("*")) {
            return new SchemaTablePrefix();
        }
        String[] parts = tableName.split("\\.");
        checkArgument(parts.length == 2, "Invalid schemaTableName: %s", tableName);
        String schema = parts[0];
        String table = parts[1];
        if (table.equals("*")) {
            return new SchemaTablePrefix(schema);
        }
        return new SchemaTablePrefix(schema, table);
    }
    
    /**
     * Execute and return the result from {@link #listExecuter()} incorporated with a timer. 
     * If listExecuter does not end within a certain amount of time (defined by 
     * the field variable fileListingTimeout) the Thread running listExecuter will 
     * be interrupted and a RuntimeException will be thrown. 
     * 
     * @param fs FileSystem, see: {@link org.apache.hadoop.fs.FileSystem}
     * @param table Table, see: {@link io.trino.plugin.hive.metastore.Table}
     * @param path  Path, see: {@link org.apache.hadoop.fs.Path}
     * @return an iterator that runs over FileStatus-Object that inclues a file's block 
     * locations
     * @throws IOException
     * @throws RuntimeException
     */
    @Override
    public RemoteIterator<LocatedFileStatus> list(FileSystem fs, Table table, Path path)
            throws IOException, RuntimeException {
        ExecutorService timeoutExecutorService = Executors.newSingleThreadExecutor();
        Future<RemoteIterator<LocatedFileStatus>> future = timeoutExecutorService
                .submit(() -> listExecuter(fs, table, path));
        try {
            return future.get(fileListingTimeout.toMillis(), TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            // the thread is interrupted, throw a RuntimeException
            System.err.println(e);
            throw new RuntimeException(e);
        } catch (ExecutionException e) {
            // error happened while executing listExecuter(), throw a RuntimeException
            System.err.println(e);
            throw new RuntimeException(e);
        } catch (TimeoutException e) {
            // timeout expired, stop this method from running and print an error message
            future.cancel(true);
            System.err.println(e);
            throw new RuntimeException(e);
        } finally {
            // shutdown the executor
            timeoutExecutorService.shutdownNow();
        }
    }

    /**
     * return an iterator over a collection whose elements need to be fetched 
     * remotely. The elements are FileStatus-objects that includes a file's block 
     * locations. 
     * @param fs FileSystem, see: {@link org.apache.hadoop.fs.FileSystem}
     * @param table Table, see: {@link io.trino.plugin.hive.metastore.Table}
     * @param path  Path, see: {@link org.apache.hadoop.fs.Path}
     * @return an iterator that runs over FileStatus-Object that inclues a file's block 
     * locations
     * @throws IOException
     */
    public RemoteIterator<LocatedFileStatus> listExecuter(FileSystem fs, Table table, Path path)
            throws IOException {
        if (!isCacheEnabledFor(table.getSchemaTableName())) {
            return fs.listLocatedStatus(path);
        }

        ValueHolder cachedValueHolder;
        try {
            cachedValueHolder = cache.get(path, ValueHolder::new);
        } catch (ExecutionException e) {
            throw new RuntimeException(e); // cannot happen
        }
        if (cachedValueHolder.getFiles().isPresent()) {
            return simpleRemoteIterator(cachedValueHolder.getFiles().get());
        }
        return cachingRemoteIterator(cachedValueHolder, fs.listLocatedStatus(path), path);
    }

    @Override
    public void invalidate(Table table)
    {
        if (isCacheEnabledFor(table.getSchemaTableName()) && isLocationPresent(table.getStorage())) {
            if (table.getPartitionColumns().isEmpty()) {
                cache.invalidate(new Path(table.getStorage().getLocation()));
            }
            else {
                // a partitioned table can have multiple paths in cache
                cache.invalidateAll();
            }
        }
    }

    @Override
    public void invalidate(Partition partition)
    {
        if (isCacheEnabledFor(partition.getSchemaTableName()) && isLocationPresent(partition.getStorage())) {
            cache.invalidate(new Path(partition.getStorage().getLocation()));
        }
    }

    private RemoteIterator<LocatedFileStatus> cachingRemoteIterator(ValueHolder cachedValueHolder, RemoteIterator<LocatedFileStatus> iterator, Path path)
    {
        return new RemoteIterator<>()
        {
            private final List<LocatedFileStatus> files = new ArrayList<>();

            @Override
            public boolean hasNext()
                    throws IOException
            {
                boolean hasNext = iterator.hasNext();
                if (!hasNext) {
                    // The cachedValueHolder acts as an invalidation guard. If a cache invalidation happens while this iterator goes over
                    // the files from the specified path, the eventually outdated file listing will not be added anymore to the cache.
                    cache.asMap().replace(path, cachedValueHolder, new ValueHolder(files));
                }
                return hasNext;
            }

            @Override
            public LocatedFileStatus next()
                    throws IOException
            {
                LocatedFileStatus next = iterator.next();
                files.add(next);
                return next;
            }
        };
    }

    private static RemoteIterator<LocatedFileStatus> simpleRemoteIterator(List<LocatedFileStatus> files)
    {
        return new RemoteIterator<>()
        {
            private final Iterator<LocatedFileStatus> iterator = ImmutableList.copyOf(files).iterator();

            @Override
            public boolean hasNext()
            {
                return iterator.hasNext();
            }

            @Override
            public LocatedFileStatus next()
            {
                return iterator.next();
            }
        };
    }

    public Duration getFileListingTimeout() {
        return fileListingTimeout;
    }

    @Managed
    public void flushCache()
    {
        cache.invalidateAll();
    }

    @Managed
    public Double getHitRate()
    {
        return cache.stats().hitRate();
    }

    @Managed
    public Double getMissRate()
    {
        return cache.stats().missRate();
    }

    @Managed
    public long getHitCount()
    {
        return cache.stats().hitCount();
    }

    @Managed
    public long getMissCount()
    {
        return cache.stats().missCount();
    }

    @Managed
    public long getRequestCount()
    {
        return cache.stats().requestCount();
    }

    @VisibleForTesting
    boolean isCached(Path path)
    {
        ValueHolder cached = cache.getIfPresent(path);
        return cached != null && cached.getFiles().isPresent();
    }

    private boolean isCacheEnabledFor(SchemaTableName schemaTableName)
    {
        return tablePrefixes.stream().anyMatch(prefix -> prefix.matches(schemaTableName));
    }

    private static boolean isLocationPresent(Storage storage)
    {
        // Some Hive table types (e.g.: views) do not have a storage location
        return storage.getOptionalLocation().isPresent() && isNotEmpty(storage.getLocation());
    }

    /**
     * The class enforces intentionally object identity semantics for the value holder,
     * not value-based class semantics to correctly act as an invalidation guard in the
     * cache.
     */
    private static class ValueHolder
    {
        private final Optional<List<LocatedFileStatus>> files;

        public ValueHolder()
        {
            files = Optional.empty();
        }

        public ValueHolder(List<LocatedFileStatus> files)
        {
            this.files = Optional.of(ImmutableList.copyOf(requireNonNull(files, "files is null")));
        }

        public Optional<List<LocatedFileStatus>> getFiles()
        {
            return files;
        }
    }
}
