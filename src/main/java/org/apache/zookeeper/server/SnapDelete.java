package org.apache.zookeeper.server;

import static org.apache.zookeeper.server.persistence.FileSnap.SNAP_MAGIC;
import me.tongfei.progressbar.ProgressBar;
import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.zip.Adler32;
import java.util.zip.CheckedInputStream;
import org.apache.jute.BinaryInputArchive;
import org.apache.jute.InputArchive;
import org.apache.zookeeper.AsyncCallback;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.Op;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.server.persistence.FileHeader;

public class SnapDelete implements Watcher {
  private ZooKeeper zk;
  private final ReferenceCountedACLCache aclCache = new ReferenceCountedACLCache();
  private String znodeToDelete;
  private ArrayList<String> znodesToDelete;
  private long numberOfZnodes = 0;
  private int numberOfZnodesToDelete = 0;
  private String digestAuth;
  private int batchSize = 1000;

  public static void main(String[] args) throws IOException, InterruptedException {
    if (args.length < 3) {
      System.err.println("USAGE: SnapDelete snapshot_file zookeeper_node znode [digestAuth]");
      System.exit(2);
    }

    SnapDelete sd = new SnapDelete(args[2]);
    if (args.length > 3) {
      sd.setDigestAuth(args[3]);
    }
    sd.run(args[0], args[1]);
  }

  private SnapDelete(String znode) {
    this.znodeToDelete = znode;
  }

  private void setDigestAuth(String authInfo) {
    digestAuth = authInfo;
  }

  private void run(String snapshotFile, String zkHostname) throws IOException, InterruptedException {
    // Deserialize snapshot
    loadSnapshot(snapshotFile);

    // Connect to ZooKeeper
    zk = new ZooKeeper(zkHostname, 30000, this);
    if (digestAuth != null) {
      zk.addAuthInfo("digest", digestAuth.getBytes());
    }
    System.out.println("\n*** Connected to ZooKeeper");

    // Delete nodes
    deleteTree();

    zk.close();
  }

  // Watcher
  public void process(WatchedEvent watchedEvent) {
    System.out.println(watchedEvent);
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
    numberOfZnodes = 0;
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
      int rateLimit = 10;
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
            // fail fast
            break;
          }
          context.sem.acquire();
          zk.multi(ops, cb, context);
          pb.stepBy(ops.size());
          ops = new ArrayList<>();
        }
      }
    }
  }

  private void delete(String znode) throws InterruptedException {
    try {
      zk.delete(znode, -1);

    } catch (KeeperException e) {
      if (e.code() != KeeperException.Code.NONODE) {
        System.out.println(znode + ": " + e.getMessage());
      }
    }
  }
}
