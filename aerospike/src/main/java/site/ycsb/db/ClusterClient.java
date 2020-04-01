package site.ycsb.db;

import com.aerospike.client.AerospikeClient;
import com.aerospike.client.*;
import com.aerospike.client.cluster.Cluster;
import com.aerospike.client.cluster.Node;
import com.aerospike.client.command.BatchExecutor;
import com.aerospike.client.command.Command;
import com.aerospike.client.policy.*;
import site.ycsb.ByteIterator;
import site.ycsb.DBException;

import java.io.Closeable;
import java.lang.reflect.Field;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

import static com.aerospike.client.command.Buffer.littleBytesToInt;
import static java.util.Arrays.asList;

public class ClusterClient implements Closeable {
  private static final String DEFAULT_HOST = "localhost";
  private static final String DEFAULT_PORT = "3000";
  private static final String DEFAULT_TIMEOUT = "10000";
  private static final String HOST_DELIM = ",";

  private final Policy readPolicy;
  private final WritePolicy insertPolicy, updatePolicy, deletePolicy;
  private final int numClusters;
  private final AerospikeClient[] readClients, writeClients;
  private final AtomicInteger readClientIndex = new AtomicInteger(-1);

  static ClusterClient newInstance(Properties props) throws DBException {
    String host = props.getProperty("as.host", DEFAULT_HOST);
    String user = props.getProperty("as.user");
    String password = props.getProperty("as.password");
    String dcLocalhosts = props.getProperty("as.local.dc.hosts");
    int port = Integer.parseInt(props.getProperty("as.port", DEFAULT_PORT));
    int timeout = Integer.parseInt(props.getProperty("as.timeout", DEFAULT_TIMEOUT));

    ClusterClient clusterClient = new ClusterClient(user, password, timeout, host, port);
    if (dcLocalhosts != null && !dcLocalhosts.trim().isEmpty()) {
      HashSet<String> localDCHosts = new HashSet<>(asList(dcLocalhosts.split(HOST_DELIM)));
      clusterClient.closeRemainingNodes(clusterClient.readClients, localDCHosts);
    }
    return clusterClient;
  }

  Record get(String namespace, String table, String key, Set<String> fields) {
    Key keyObj = new Key(namespace, table, key);
    AerospikeClient readClient = chooseReadClient(keyObj);
    if (fields != null) {
      return readClient.get(readPolicy, keyObj, fields.toArray(new String[0]));
    } else {
      return readClient.get(readPolicy, keyObj);
    }
  }

  Record[] get(String namespace, String table, String[] keys) {
    int numKeys = keys.length;
    if (numKeys == 0) return null;

//    return getRecordsUsingRoundRobin(namespace, table, keys);
    return getRecordsUsingKeyPartitions(namespace, table, keys);
  }

  private Record[] getRecordsUsingRoundRobin(String namespace, String table, String[] keys) {
    int numKeys = keys.length;
    Key[] keyObjs = new Key[numKeys];
    for (int i = 0; i < numKeys; i++) {
      keyObjs[i] = new Key(namespace, table, keys[i]);
    }
    BatchPolicy batchPolicy = new BatchPolicy(this.readPolicy);
    AerospikeClient readClient = getRoundRobinClient(readClients, readClientIndex);
    return getRecords(readClient, batchPolicy, keyObjs);
  }

  private Record[] getRecordsUsingKeyPartitions(String namespace, String table, String[] keys) {
    LinkedList[] keyClusters = new LinkedList[numClusters];
    for (String key : keys) {
      Key keyObj = new Key(namespace, table, key);
      int clIdx = chooseClusterIdx(keyObj);
      if (keyClusters[clIdx] == null) {
        keyClusters[clIdx] = new LinkedList<>();
      }
      //noinspection unchecked
      keyClusters[clIdx].add(keyObj);
    }

    BatchPolicy batchPolicy = new BatchPolicy(this.readPolicy);
    LinkedList<Record> records = new LinkedList<>();
    for (int i = 0; i < numClusters; i++) {
      LinkedList keyCluster = keyClusters[i];
      if (keyCluster != null) {
        Key[] keyObjs = (Key[]) keyCluster.toArray(new Key[0]);
        AerospikeClient readClient = readClients[i];
        Record[] batchRecords = getRecords(readClient, batchPolicy, keyObjs);
        Collections.addAll(records, batchRecords);
      }
    }
    return records.toArray(new Record[0]);
  }

  private AerospikeClient getRoundRobinClient(AerospikeClient[] clients, AtomicInteger clientIndex) {
    int numClients = clients.length;
    int index = clientIndex.updateAndGet(op -> (op + 1) % numClients);
    return clients[index];
  }

  private Record[] getRecords(AerospikeClient readClient, BatchPolicy batchPolicy, Key[] keyObjs) {
    Record[] batchRecords = new Record[keyObjs.length];
    Cluster cluster = loadCluster(readClient);
    BatchExecutor.execute(cluster, batchPolicy, keyObjs, null, batchRecords, null, Command.INFO1_READ | Command.INFO1_GET_ALL);
    return batchRecords;
  }

  private Cluster loadCluster(AerospikeClient client) {
    try {
      Field clusterField = AerospikeClient.class.getDeclaredField("cluster");
      clusterField.setAccessible(true);
      return (Cluster) clusterField.get(client);
    } catch (IllegalAccessException | NoSuchFieldException e) {
      throw new AerospikeException(e);
    }
  }

  void insert(String namespace, String table, String key, Map<String, ByteIterator> values) {
    Key keyObj = new Key(namespace, table, key);
    chooseWriteClient(keyObj).put(insertPolicy, keyObj, toBins(values));
  }

  void update(String namespace, String table, String key, Map<String, ByteIterator> values) {
    Key keyObj = new Key(namespace, table, key);
    chooseWriteClient(keyObj).put(updatePolicy, keyObj, toBins(values));
  }

  boolean delete(String namespace, String table, String key) {
    Key keyObj = new Key(namespace, table, key);
    return chooseWriteClient(keyObj).delete(deletePolicy, keyObj);
  }

  @Override
  public void close() {
    for (int i = 0; i < numClusters; i++) {
      readClients[i].close();
      writeClients[i].close();
    }
  }

  private ClusterClient(String user, String password, int timeout, String host, int port) throws DBException {
    readPolicy = new Policy();
    insertPolicy = new WritePolicy();
    updatePolicy = new WritePolicy();
    deletePolicy = new WritePolicy();

    insertPolicy.recordExistsAction = RecordExistsAction.CREATE_ONLY;
    updatePolicy.recordExistsAction = RecordExistsAction.REPLACE_ONLY;

    readPolicy.replica = Replica.MASTER_PROLES;
    insertPolicy.commitLevel = CommitLevel.COMMIT_ALL;
    updatePolicy.commitLevel = CommitLevel.COMMIT_ALL;
    deletePolicy.commitLevel = CommitLevel.COMMIT_ALL;

    readPolicy.setTimeout(timeout);
    insertPolicy.setTimeout(timeout);
    updatePolicy.setTimeout(timeout);
    deletePolicy.setTimeout(timeout);

    ClientPolicy clientPolicy = new ClientPolicy();

    if (user != null && password != null) {
      clientPolicy.user = user;
      clientPolicy.password = password;
    }

    try {
      String[] hosts = host.split(HOST_DELIM);
      numClusters = hosts.length;
      readClients = new AerospikeClient[numClusters];
      writeClients = new AerospikeClient[numClusters];
      for (int i = 0; i < hosts.length; i++) {
        String hostname = hosts[i].trim();
        readClients[i] = new AerospikeClient(clientPolicy, hostname, port);
        writeClients[i] = new AerospikeClient(clientPolicy, hostname, port);
      }
    } catch (AerospikeException e) {
      throw new DBException(String.format("Error while creating Aerospike " +
          "client for %s:%d.", host, port), e);
    }
  }

  private void closeRemainingNodes(AerospikeClient[] aerospikeClient, Set<String> hosts) {
    for (AerospikeClient client : aerospikeClient) {
      for (Node node : client.getNodes()) {
        String nodeName = node.getHost().name;
        if (!hosts.contains(nodeName)) {
          node.close();
          System.err.println("[WARN] Closed connection to remote DC host: " + nodeName);
        }
      }
    }
  }

  private int chooseClusterIdx(Key key) {
    return unsignedHash(key) % numClusters;
  }

  private AerospikeClient chooseReadClient(Key key) {
    return readClients[chooseClusterIdx(key)];
  }

  private AerospikeClient chooseWriteClient(Key key) {
    return writeClients[chooseClusterIdx(key)];
  }

  private int unsignedHash(Key key) {
    return littleBytesToInt(key.digest, 0) & 0xFFFF0000 >>> 16;
  }

  private Bin[] toBins(Map<String, ByteIterator> values) {
    Bin[] bins = new Bin[values.size()];
    int index = 0;

    for (Map.Entry<String, ByteIterator> entry : values.entrySet()) {
      bins[index] = new Bin(entry.getKey(), entry.getValue().toArray());
      ++index;
    }
    return bins;
  }
}
