package org.apache.zookeeper.server;

import static org.apache.zookeeper.server.persistence.FileSnap.SNAP_MAGIC;

import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Stack;
import java.util.zip.Adler32;
import java.util.zip.CheckedInputStream;
import org.apache.jute.BinaryInputArchive;
import org.apache.jute.InputArchive;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.server.persistence.FileHeader;

public class SnapDelete implements Watcher {
  private ZooKeeper zk;
  private final ReferenceCountedACLCache aclCache = new ReferenceCountedACLCache();
  private String znodeToDelete;
  private Stack<String> znodesToDelete;

  public static void main(String[] args) throws IOException, InterruptedException {
    System.out.println(System.getenv("CLASSPATH"));

    if (args.length != 3) {
      System.err.println("USAGE: SnapDelete snapshot_file zookeeper_node znode");
      System.exit(2);
    }

    new SnapDelete(args[2]).run(args[0], args[1]);
  }

  private SnapDelete(String znode) {
    this.znodeToDelete = znode;
  }

  private void run(String snapshotFile, String zkHostname) throws IOException, InterruptedException {
    // Connect to ZooKeeper
    zk = new ZooKeeper(zkHostname, 30000, this);
    System.out.println("\n*** Connected to ZooKeeper with session timeout " + zk.getSessionTimeout() + " ms");

    // Deserialize snapshot
    loadSnapshot(snapshotFile);

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
      long id = ia.readLong("id");
      int to = ia.readInt("timeout");
      //sessions.put(id, to);
      count--;
    }

    // ACL cache
    aclCache.deserialize(ia);

    deserialize(ia, "tree");

    System.out.println("\n*** Snapshot loaded.");
  }

  private void deserialize(InputArchive ia, String tag) throws IOException, InterruptedException {
    long num = 0;
    String path = ia.readString("path");
    System.out.println(path);
    DataNode node = new DataNode();

    znodesToDelete = new Stack<String>();
    znodesToDelete.push(this.znodeToDelete);
    ++num;

    while (!path.equals("/")) {
      if (path.startsWith(znodeToDelete + "/")) {
        znodesToDelete.push(path);
      }

      ia.readRecord(node, "node");
      path = ia.readString("path");
      System.out.print("\r" + num);
      System.out.flush();
    }

    System.out.println();
  }

  private void deleteTree() throws InterruptedException {
    System.out.println("\n*** Deleting subtree");
    while (!znodesToDelete.empty()) {
      String znode = znodesToDelete.pop();
      delete(znode);
    }
  }

  private void delete(String znode) throws InterruptedException {
    try {
      zk.delete(znode, -1);
      System.out.println(znode + ": deleted");
    } catch (KeeperException e) {
      System.out.println(znode + ": " + e.getMessage());
    }
  }
}
