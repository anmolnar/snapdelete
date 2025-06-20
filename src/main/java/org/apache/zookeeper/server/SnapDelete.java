package org.apache.zookeeper.server;

import me.tongfei.progressbar.ProgressBar;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.jute.BinaryInputArchive;
import org.apache.jute.InputArchive;
import org.apache.zookeeper.AsyncCallback;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.Op;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.server.persistence.FileHeader;

import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.zip.Adler32;
import java.util.zip.CheckedInputStream;

import static org.apache.zookeeper.server.persistence.FileSnap.SNAP_MAGIC;

public class SnapDelete implements Watcher {
  private String zkHostname;
  private String snapshotFile;
  private ZooKeeper zk;
  private final ReferenceCountedACLCache aclCache = new ReferenceCountedACLCache();
  private String znodeToDelete;
  private ArrayList<String> znodesToDelete;
  private int numberOfZnodesToDelete = 0;
  private String digestAuth;
  private int batchSize = 1000;
  private int rateLimit = 10;
  private CountDownLatch connected = new CountDownLatch(1);

  public static void main(String[] args) throws IOException, InterruptedException {
    SnapDelete sd = new SnapDelete();
    sd.parseCommandLineArguments(args);
    sd.dumpConfig();
    sd.run();
  }

  private void run() throws IOException, InterruptedException {
    // Deserialize snapshot
    loadSnapshot(snapshotFile);

    // Connect to ZooKeeper
    connectToZK(zkHostname);

    // Delete nodes
    deleteTree();

    zk.close();
  }

  private void connectToZK(String zkHostname) throws IOException, InterruptedException {
    this.zkHostname = zkHostname;
    connected = new CountDownLatch(1);
    zk = new ZooKeeper(zkHostname, 30000, this);
    if (digestAuth != null) {
      zk.addAuthInfo("digest", digestAuth.getBytes());
    }
    connected.await();
    System.out.println("\n*** Connected to ZooKeeper");
  }

  // Watcher
  public void process(WatchedEvent watchedEvent) {
    System.out.println(watchedEvent);
    if (watchedEvent.getState() == Event.KeeperState.SyncConnected) {
      connected.countDown();
    }
  }

  private void loadSnapshot(String snapshotFileName) throws IOException, InterruptedException {
    System.out.println("\n*** Reading snapshot: " + snapshotFileName);

    InputStream is = new CheckedInputStream(
        new BufferedInputStream(new FileInputStream(snapshotFileName)),
        new Adler32());
    InputArchive ia = BinaryInputArchive.getArchive(is);

    // Header
    FileHeader header = new FileHeader();
    header.deserialize(ia, "fileheader");
    if (header.getMagic() != SNAP_MAGIC) {
      throw new IOException("mismatching magic headers "
          + header.getMagic() +
          " !=  " + SNAP_MAGIC);
    }

    // Sessions
    int count = ia.readInt("count");
    while (count > 0) {
      ia.readLong("id");
      ia.readInt("timeout");
      count--;
    }

    // ACL cache
    aclCache.deserialize(ia);

    deserialize(ia, "tree");

    System.out.println("\n*** Snapshot loaded.");
  }

  private void deserialize(InputArchive ia, String tag) throws IOException {
    long numberOfZnodes = 0;
    numberOfZnodesToDelete = 0;

    String path = ia.readString("path");
    System.out.println(path);
    DataNode node = new DataNode();

    znodesToDelete = new ArrayList<>(45 * 1000 * 1000);
    znodesToDelete.add(this.znodeToDelete);

    ++numberOfZnodesToDelete;
    ++numberOfZnodes;

    while (!path.equals("/")) {
      if (path.startsWith(znodeToDelete + "/")) {
        znodesToDelete.add(path);
        ++numberOfZnodesToDelete;
      }

      ia.readRecord(node, "node");
      path = ia.readString("path");
      if (++numberOfZnodes % 1000 == 0) {
        System.out.print("\rZnodes loaded: " + numberOfZnodes);
        System.out.flush();
      }
    }

    System.out.println("\n*** Znodes loaded: " + numberOfZnodes);
    System.out.println("*** Znodes to delete: " + numberOfZnodesToDelete);
  }

  private static class BatchedDeleteCbContext {
    Semaphore sem;
    AtomicBoolean success;

    BatchedDeleteCbContext(int rateLimit) {
      sem = new Semaphore(rateLimit);
      success = new AtomicBoolean(true);
    }
  }

  private void deleteTree() throws InterruptedException {
    System.out.println("\n*** Deleting subtree: " + numberOfZnodesToDelete + " znodes");
    try (ProgressBar pb = new ProgressBar("Deleting", numberOfZnodesToDelete)) {
      List<Op> ops = new ArrayList<>();
      BatchedDeleteCbContext context = new BatchedDeleteCbContext(rateLimit);
      AsyncCallback.MultiCallback cb = (rc, path, ctx, opResults) -> {
        ((BatchedDeleteCbContext) ctx).sem.release();
        if (!(rc == KeeperException.Code.OK.intValue() || rc == KeeperException.Code.NONODE.intValue())) {
          ((BatchedDeleteCbContext) ctx).success.set(false);
        }
      };

      for (int i = numberOfZnodesToDelete - 1; i >= 0; --i) {
        String znode = znodesToDelete.get(i);
        ops.add(Op.delete(znode, -1));

        if (ops.size() == batchSize || i == 0) {
          if (!context.success.get()) {
            // Try to reconnect
            if (zk.getState() != ZooKeeper.States.CONNECTED) {
              connectToZK(zkHostname);
            }
          }
          context.sem.acquire();
          zk.multi(ops, cb, context);
          pb.stepBy(ops.size());
          ops = new ArrayList<>();
        }
      }
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  private void parseCommandLineArguments(String[] args) {
    Options options = new Options();

    options.addOption("a", "auth", true, "Digest authentication string (default: noauth)");
    options.addOption("b", "batch", true, "Batch size (default: 1000)");
    options.addOption("r", "rate",true, "Rate control, number of simultaneous batches (default: 10)");

    CommandLineParser parser = new DefaultParser();
    HelpFormatter formatter = new HelpFormatter();
    CommandLine cmd;

    try {
      cmd = parser.parse(options, args);
      String[] posArgs = cmd.getArgs();
      if (posArgs.length != 3) {
        throw new ParseException("Must have exactly 3 positional arguments");
      }
      digestAuth = cmd.getOptionValue("auth");
      if (cmd.getOptionValue("batch") != null) {
        batchSize = Integer.parseInt(cmd.getOptionValue("batch"));
      }
      if (cmd.getOptionValue("rate") != null) {
        rateLimit = Integer.parseInt(cmd.getOptionValue("rate"));
      }
      snapshotFile = posArgs[0];
      zkHostname = posArgs[1];
      znodeToDelete = posArgs[2];
    } catch (Exception e) {
      System.out.println(e.getMessage());
      formatter.printHelp("SnapDelete -[abr] snapshot_file zookeeper_host znode", options);
      System.exit(1);
    }
  }

  private void dumpConfig() {
    System.out.println("\n*** Config dump\n");
    System.out.println("Snapshot file: " + snapshotFile);
    System.out.println("ZooKeeper hostname: " + zkHostname);
    System.out.println("Znode to delete: " + znodeToDelete);
    if (digestAuth != null) {
      System.out.println("Digest authentication set: " + digestAuth);
    } else {
      System.out.println("Authentication is OFF");
    }
    System.out.println("Batch size: " + batchSize);
    System.out.println("Rate limit: " + rateLimit);
  }
}
