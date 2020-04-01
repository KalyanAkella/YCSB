/**
 * Copyright (c) 2015 YCSB contributors. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you
 * may not use this file except in compliance with the License. You
 * may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied. See the License for the specific language governing
 * permissions and limitations under the License. See accompanying
 * LICENSE file.
 */

package site.ycsb.db;

import com.aerospike.client.AerospikeException;
import com.aerospike.client.Record;
import site.ycsb.ByteArrayByteIterator;
import site.ycsb.ByteIterator;
import site.ycsb.DBException;
import site.ycsb.Status;

import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import static java.lang.String.format;
import static java.util.Collections.singletonMap;

/**
 * YCSB binding for <a href="http://www.aerospike.com/">Areospike</a>.
 */
public class AerospikeClient extends site.ycsb.DB {
  private static final String DEFAULT_NAMESPACE = "ycsb";
  private static final String KEY_FORMAT = "%s_%s";
  private static final ExecutorService THREAD_POOL = Executors.newCachedThreadPool();

  private String namespace;
  private ClusterClient clusterClient;

  @Override
  public void init() throws DBException {
    Properties props = getProperties();
    namespace = props.getProperty("as.namespace", DEFAULT_NAMESPACE);
    clusterClient = ClusterClient.newInstance(props);
  }

  @Override
  public void cleanup() throws DBException {
    clusterClient.close();
  }

  @Override
  public Status read(String table, String key, Set<String> fields,
      Map<String, ByteIterator> result) {
    try {
      String[] keys = new String[fields.size()];
      int idx = 0;
      for (String field : fields) {
        keys[idx++] = format(KEY_FORMAT, key, field);
      }
      Record[] records = clusterClient.get(namespace, table, keys);

      for (Record record : records) {
        if (record == null) {
          System.err.println("Record key " + key + " not found (read)");
          return Status.ERROR;
        }
        for (Map.Entry<String, Object> entry: record.bins.entrySet()) {
          result.put(entry.getKey(), new ByteArrayByteIterator((byte[])entry.getValue()));
        }
      }
      return Status.OK;
    } catch (AerospikeException e) {
      e.printStackTrace();
      return Status.ERROR;
    }
  }

  @Override
  public Status scan(String table, String start, int count, Set<String> fields,
      Vector<HashMap<String, ByteIterator>> result) {
    System.err.println("Scan not implemented");
    return Status.ERROR;
  }

  @Override
  public Status update(String table, String key,
                       Map<String, ByteIterator> values) {
    try {
      Future<?>[] futures = new Future[values.size()];
      int idx = 0;
      for (Map.Entry<String, ByteIterator> entry : values.entrySet()) {
        String customKey = format(KEY_FORMAT, key, entry.getKey());
        Map<String, ByteIterator> value = singletonMap(entry.getKey(), entry.getValue());
        futures[idx++] = THREAD_POOL.submit(() -> clusterClient.update(namespace, table, customKey, value));
      }

      for (Future<?> future : futures) {
        future.get();
      }
      return Status.OK;
    } catch (Exception e) {
      e.printStackTrace();
      return Status.ERROR;
    }
  }

  @Override
  public Status insert(String table, String key,
                       Map<String, ByteIterator> values) {
    try {
      Future<?>[] futures = new Future[values.size()];
      int idx = 0;
      for (Map.Entry<String, ByteIterator> entry : values.entrySet()) {
        String customKey = format(KEY_FORMAT, key, entry.getKey());
        Map<String, ByteIterator> value = singletonMap(entry.getKey(), entry.getValue());
        futures[idx++] = THREAD_POOL.submit(() -> clusterClient.insert(namespace, table, customKey, value));
      }

      for (Future<?> future : futures) {
        future.get();
      }
      return Status.OK;
    } catch (Exception e) {
      e.printStackTrace();
      return Status.ERROR;
    }
  }

  @Override
  public Status delete(String table, String key) {
    try {
      if (!clusterClient.delete(namespace, table, key)) {
        System.err.println("Record key " + key + " not found (delete)");
        return Status.ERROR;
      }
      return Status.OK;
    } catch (AerospikeException e) {
      e.printStackTrace();
      return Status.ERROR;
    }
  }
}
