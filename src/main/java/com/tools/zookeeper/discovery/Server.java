package com.tools.zookeeper.discovery;

import org.apache.zookeeper.*;

import java.io.IOException;

/**
 * 每次启动后，在执行自己的核心业务之前，先向zk集群注册一个临时节点
 *  且向临时节点中保存一些关键信息
 * */

public class Server {
    private String connectString = "localhost:2181,localhost:2182";
    private int  sessionTimeout = 6000;
    private ZooKeeper zooKeeper;

    private String basePath = "/Servers";  // 生成临时节点的副路径

    // 初始化客户端对象
    public void init() throws IOException {
        zooKeeper = new ZooKeeper(connectString, sessionTimeout, new Watcher() {
            @Override
            public void process(WatchedEvent event) {

            }
        });
    }

    // 使用zk客户端注册临时节点
    public void regist(String info) throws InterruptedException, KeeperException {
        // 节点必须是临时带序号的节点
        zooKeeper.create(basePath+"/server", info.getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);

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
        // 传入参数，模拟服务注册。 分别传入 pay 198.162.100.1   check 198.162.100.2 执行两个进程
        // Routor监听器会持续监听节点，并将变化后的节点打印
        Server server = new Server();
        server.init();
        server.regist(args[0]);
        server.doOtherBusiness();
    }
}
