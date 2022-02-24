package com.tools.zookeeper.discovery;
import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 *
 * 从zk集群获取当前启动的Server进程有哪些，获取到Server进程的信息
 *   持续监听Server进程的变化，一旦有变化，重新获取Server进程的信息
 * */

public class Routor {
    private String connectString = "localhost:2181,localhost:2182";
    private int  sessionTimeout = 6000;
    private ZooKeeper zooKeeper;

    private String basePath = "/Servers";

    public void init() throws IOException {
        zooKeeper = new ZooKeeper(connectString, sessionTimeout, new Watcher() {
            @Override
            public void process(WatchedEvent event) {

            }
        });
    }

    // 检查/Servers这个节点是否存在，如果不存在，需要创建这个节点
    public void check() throws InterruptedException, KeeperException {
        Stat stat = zooKeeper.exists(basePath, false);
        // 不存在，初始化根节点
        if (stat == null) {
            // /Servers必须是永久节点
            zooKeeper.create(basePath, "".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        }
    }

    // 获取当前启动的Server进程有哪些，获取到Server进程的信息
    public List<String> getData() throws InterruptedException, KeeperException {

        List<String> result = new ArrayList<>();

        List<String> children = zooKeeper.getChildren(basePath, new Watcher() {
            // 递归，持续监听
            @Override
            public void process(WatchedEvent event) {
                System.out.println(event.getPath()+"发生了以下事件："+event.getType());
                try {
                    getData();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                } catch (KeeperException e) {
                    e.printStackTrace();
                }
            }
        });

        // 获取每个节点中保存的Server的信息
        for (String child : children) {
            byte[] info = zooKeeper.getData(basePath + "/" + child, null, null);
            result.add(new String(info));
        }
        System.out.println("最新读到的信息是："+result);
        return result;
    }
    // 其他的业务功能
    public void doOtherBusiness() throws InterruptedException {
        System.out.println("working.......");

        // 持续工作
        while (true) {
            Thread.sleep(5000);
            System.out.println("working.......");
        }
    }
    public static void main(String[] args) throws IOException, InterruptedException, KeeperException {
        Routor routor = new Routor();
        routor.init();
        routor.check();
        routor.getData();
        routor.doOtherBusiness();
    }
}
