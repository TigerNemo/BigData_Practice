package com.tools.zookeeper.zkwatch;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.CountDownLatch;

/**
 * 监听器的原理：
 * 初始化zkClient的时候会初始化两个线程，一个是Listener线程，一个是Connect线程。
 * Connect线程负责向zookeeper集群发命令，执行相关的动作。如果使用监听器的相关参数，就会在zookeeper中注册一个监听器。
 * 一旦要监听的路径发生变化，就会向Listener线程发消息。
 * Listener线程收到消息之后就会调用process()方法，执行相关操作。
 * Listener线程是不能阻塞的，阻塞后就无法再调用process()
 * */

public class ZKWatch {
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

    // ls path watch
    @Test
    public void lsAndWatch() throws Exception {
        // 传入true，默认使用客户端自带的观察者
        // 一般来说不用自带的观察者，因为多个方法会同时调同一个process()。所以要自定义观察者
        zooKeeper.getChildren("/idea", new Watcher() {
            // 当前线程自己设置的观察者
            @Override
            public void process(WatchedEvent event) {
                System.out.println(event.getPath()+"发生了以下事件："+event.getType());
                try {
                    List<String> children = zooKeeper.getChildren("/idea",null);
                    // 重新查询当前路径的所有新的节点
                    System.out.println(event.getPath()+"的新节点："+ children);
                } catch (KeeperException e) {
                    e.printStackTrace();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        });

        // 客户端所在的进程不能死亡
        while (true) {
            Thread.sleep(5000);
            System.out.println("我还活着.......");
        }
    }

    private CountDownLatch countDownLatch = new CountDownLatch(1);

    @Test
    public void getAndWatch() throws InterruptedException, KeeperException {

        // 是Connect线程调用
        byte[] data = zooKeeper.getData("/idea", new Watcher() {

            // 是Listener线程调用
            @Override
            public void process(WatchedEvent event) {
                System.out.println(event.getPath()+"发生了以下事件："+event.getType());
                // 减一
                countDownLatch.countDown();
            }
        }, null);

        System.out.println("查询到的数据是："+new String(data));

        // 阻塞当前线程，当初始化的值变成0时，当前线程会唤醒
        countDownLatch.await();
    }


    // 持续监听
    @Test
    public void lsAndAlwaysWatch() throws Exception {
        lsAndAlwaysWatchCurrent();

        // 客户端所在的进程不能死亡
        // 要单独写一个回调函数lsAndAlwaysWatchCurrent()来实现递归调用，不然会阻塞到客户端进程的while(true)中，无法再次调用。
        while (true) {
            Thread.sleep(5000);
            System.out.println("我还活着.......");
        }
    }
    @Test
    public void lsAndAlwaysWatchCurrent() throws InterruptedException, KeeperException {
        // 传入true，默认使用客户端自带的观察者
        // 一般来说不用自带的观察者，因为多个方法会同时调同一个process()。所以要自定义观察者
        zooKeeper.getChildren("/idea", new Watcher() {
            // 当前线程自己设置的观察者
            @Override
            public void process(WatchedEvent event) {
                System.out.println(event.getPath()+"发生了以下事件："+event.getType());
                try {
                    List<String> children = zooKeeper.getChildren("/idea",null);
                    // 重新查询当前路径的所有新的节点
                    System.out.println(event.getPath()+"的新节点："+ children);
                    lsAndAlwaysWatchCurrent();
                } catch (KeeperException e) {
                    e.printStackTrace();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        });
    }
}
