package com.tools.zookeeper.zkclint;

import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.List;

public class ZKClint {
    private String connectString = "localhost:2181,localhost:2182";
    private int  sessionTimeout = 6000;
    private ZooKeeper zooKeeper;

    // zkCLi.sh -server xxx:2181
    @Before
    public void init() throws IOException {
        // 创建一个zk的客户端对象
        zooKeeper = new ZooKeeper(connectString, sessionTimeout, new Watcher() {
            // 回调方法，一旦watcher观察的path触发了指定的事件，服务端会通知客户端，客户端收到通知后
            // 会自动调用process()
            @Override
            public void process(WatchedEvent event) {

            }
        });
        System.out.println(zooKeeper);
    }

    @After
    public void close() throws InterruptedException {
        if (zooKeeper != null) {
            zooKeeper.close();
        }
    }

    // ls
    @Test
    public void ls() throws InterruptedException, KeeperException {
        Stat stat = new Stat();  // 查看状态
        List<String> children = zooKeeper.getChildren("/", null);
        System.out.println(children);
        System.out.println(stat);
    }

    // create [-s] [-e] path data
    @Test
    public void create() throws InterruptedException, KeeperException {
        zooKeeper.create("/idea", "hello".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
    }

    // get path
    @Test
    public void get() throws InterruptedException, KeeperException {
        byte[] data = zooKeeper.getData("/idea", null, null);
        System.out.println(new String(data));
    }

    // set path data
    @Test
    public void set() throws InterruptedException, KeeperException {
        zooKeeper.setData("/idea", "hi".getBytes(), -1);
    }

    // delete path
    @Test
    public void delete() throws InterruptedException, KeeperException {
        zooKeeper.delete("/idea",-1);
    }

    // rmr path：递归删除，删除当前节点及所有子节点
    @Test
    public void rmr() throws InterruptedException, KeeperException {
        String path = "/data1";  // 两层
        // 先获取当前路径中所有的子node
        List<String> children = zooKeeper.getChildren(path, false);
        // 删除所有子节点
        for (String child : children) {
            zooKeeper.delete(path + "/" + child, -1);
        }
        zooKeeper.delete(path,-1);
    }

    // 判断当前节点是否存在
    @Test
    public void ifNodeExists() throws InterruptedException, KeeperException {
        Stat stat = zooKeeper.exists("/zookeeper", false);
        System.out.println(stat==null ? "不存在" : "存在");
    }
}
