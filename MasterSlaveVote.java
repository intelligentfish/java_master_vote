package com.neo.wang.zktest;

import lombok.SneakyThrows;
import org.apache.zookeeper.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.function.Function;

/**
 * 主从选举
 */
public class MasterSlaveVote implements Watcher {
    /**
     * 主节点处理器
     */
    public interface MasterHandler {
        /**
         * 处理
         */
        void handle();
    }

    private static final Logger LOGGER = LoggerFactory.getLogger(MasterSlaveVote.class);// 日志
    private volatile boolean connectedFlag; // 连接标志
    private String zkAddress; // ZK地址
    private int zkTimeout; // ZK超时
    private String topDir; // 顶层目录
    private String masterNodeFullPath; // 主节点全路径
    private ZooKeeper zk; // ZK客户端
    private volatile boolean isMaster; // 是否主节点
    private CountDownLatch connectedCountDownLatch; // 连接计数器
    private List<MasterHandler> masterHandlers; // 主节点

    /**
     * 目录迭代
     *
     * @param path     全路径 eg. /ip/127.0.0.1:1001
     * @param callback 路径回调 /ip /ip/127.0.0.0:1001
     * @return
     */
    private Throwable pathIterator(String path, Function<String, Throwable> callback) {
        int end = path.indexOf("/", '/' == path.charAt(0) ? 1 : 0);
        for (; -1 != end; ) {
            Throwable t = callback.apply(path.substring(0, end));
            if (null != t) return t;
            if ('/' == path.charAt(end)) end++;
            end = path.indexOf("/", end);
        }
        return callback.apply(path);
    }

    /**
     * 创建并监听结点
     *
     * @throws Throwable
     */
    private void createAndWatchNode() throws Throwable {
        String[] paths = new String[1];
        String nodePath = String.format("%s/node", this.topDir);
        Throwable tx = this.pathIterator(nodePath, currentPath -> {
            boolean equals = currentPath.equals(nodePath);
            try {
                if (null != this.zk.exists(currentPath, false)) return null;
                paths[0] = this.zk.create(currentPath,
                        null,
                        ZooDefs.Ids.OPEN_ACL_UNSAFE,
                        equals ? CreateMode.EPHEMERAL_SEQUENTIAL : CreateMode.PERSISTENT);
            } catch (Throwable t) {
                return t;
            }
            return null;
        });
        if (null != tx) throw tx;

        String myPath = paths[0];

        LOGGER.info("my path: {}, id: {}", myPath, myPath.substring(nodePath.length()));

        int nodeID = Integer.parseInt(myPath.substring(nodePath.length()));
        String fmt = String.format("%%s%%0%dd", myPath.length() - nodePath.length());
        String prevNode = String.format(fmt, nodePath, nodeID - 1);
        if (null == this.zk.exists(prevNode, true)) {
            this.masterNodeFullPath = myPath;
            this.isMaster = true;

            LOGGER.info("master path: {}", this.masterNodeFullPath);

            synchronized (this.masterHandlers) {
                if (!this.masterHandlers.isEmpty()) {
                    for (MasterHandler handler : this.masterHandlers) handler.handle();
                }
            }
        }
    }

    /**
     * 连接Zookeeper服务
     *
     * @param vote
     * @throws IOException
     * @throws InterruptedException
     */
    private void connect(boolean vote) throws IOException, InterruptedException {
        this.connectedCountDownLatch = new CountDownLatch(1);
        this.zk = new ZooKeeper(zkAddress, zkTimeout, this);
        this.connectedCountDownLatch.await();
        if (vote) this.vote();
    }

    /**
     * 处理ZK事件
     *
     * @param event ZK事件
     */
    @Override
    public void process(WatchedEvent event) {
        LOGGER.info("zk event type: {}, state: {}", event.getType(), event.getState());

        // 处理首次连接
        if (event.getType() == Event.EventType.None) {
            this.connectedFlag = event.getState() == Event.KeeperState.SyncConnected;
            this.connectedCountDownLatch.countDown();
            return;
        }

        switch (event.getState()) {
            case SyncConnected:
                switch (event.getType()) {
                    case NodeDeleted:
                        this.isMaster = true;
                        this.masterNodeFullPath = event.getPath();
                        synchronized (this.masterHandlers) {
                            if (!this.masterHandlers.isEmpty())
                                for (MasterHandler handler : this.masterHandlers) handler.handle();
                        }
                }
                break;
        }

        // 处理具体事件
        switch (event.getState()) {
            case SyncConnected: {
                // 连接
                switch (event.getType()) {
                    case NodeDeleted: {
                        this.isMaster = true;
                        this.masterNodeFullPath = event.getPath();
                        synchronized (this.masterHandlers) {
                            if (!this.masterHandlers.isEmpty())
                                for (MasterHandler handler : this.masterHandlers) handler.handle();
                        }
                    }
                    break;
                }
            }
            break;

            case Disconnected: {
                // 连接断掉
            }
            break;

            case Expired: {
                // 处理会话过期
                try {
                    this.connect(true);
                } catch (IOException e) {
                    e.printStackTrace();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
            break;
        }
    }

    /**
     * 构造方法
     *
     * @param zkAddress ZK地址
     * @param zkTimeout ZK超时
     * @param topDir    顶层目录
     */
    @SneakyThrows
    public MasterSlaveVote(String zkAddress, int zkTimeout, String topDir) {
        this.zkAddress = zkAddress;
        this.zkTimeout = zkTimeout;
        this.topDir = topDir;
        this.masterHandlers = new CopyOnWriteArrayList<>();
        this.connect(false);
    }

    /**
     * 是否主节点
     *
     * @return
     */
    public boolean isMaster() {
        return isMaster;
    }

    /**
     * 添加处理器
     *
     * @param handler 处理器
     * @return
     */
    public MasterSlaveVote addHandler(MasterHandler handler) {
        synchronized (this.masterHandlers) {
            this.masterHandlers.add(handler);
        }
        return this;
    }

    /**
     * 选举
     */
    @SneakyThrows
    public void vote() {
        synchronized (this) {
            this.createAndWatchNode();
        }
    }
}
