package com.xiaomi.infra.pegasus.client;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import sun.misc.Signal;
import sun.misc.SignalHandler;

// cmd : java -classpath ./library/*:pegasus-client-2.1-SNAPSHOT.jar -Dlog4j.configuration=file:../configuration/log4j.properties com.xiaomi.infra.pegasus.client.DeadLockRecurrence

public class DeadLockRecurrence {
  private static volatile boolean isRunning = true;

  public static class MySignalHandler implements SignalHandler {
    @Override
    public void handle(Signal signal) {
      if (signal.equals(new Signal("TERM"))) {
        isRunning = false;
        System.out.println("receive term signal......");
      } else {
        System.out.println("receive other signal......");
      }
    }
  }

  public static void main(String[] args) {
    String metaList = "10.120.147.34:8170,10.120.60.174:8170,10.120.77.101:8170";
    /*
    ClientOptions options =
        ClientOptions.builder()
            .asyncWorkers(2)
            .metaServers(metaList)
            .operationTimeout(Duration.ofMillis(8000))
            .sessionResetTimeWindowSecs(10)
            .build();
     */
    PegasusClientInterface pInterface = null;
    try {
      pInterface =
          PegasusClientFactory.getSingletonClient(
              ClientOptions.builder()
                  .metaServers(metaList)
                  .metaQueryTimeout(Duration.ofMillis(10000))
                  .enablePerfCounter(false)
                  .build());
    } catch (PException e) {
      System.out.println("throw exception when creat client :" + e);
      System.exit(-1);
    }

    // 1M data
    String bigBlob = new String("1");
    for (int i = 0; i < 5; ++i) {
      bigBlob += bigBlob;
    }

    System.out.println("bigBlog size:" + bigBlob.length());

    List<String> testKeyList = new ArrayList<>();
    for (int i = 0; i < 12; ++i) {
      testKeyList.add("test_str_" + i);
    }

    try {
      PegasusTableInterface pegasusTableInterface = pInterface.openTable("testDeadLock");
      System.out.println("open table success...");
      for (String key : testKeyList) {
        pegasusTableInterface.set(key.getBytes(), key.getBytes(), bigBlob.getBytes(), 10000);
        System.out.println("set " + key + " success...");
      }
    } catch (PException e) {
      System.out.println("throw exception when write to table :" + e);
      e.printStackTrace();
      System.exit(-1);
    }

    List<Thread> threadList = new ArrayList<>();
    final PegasusClientInterface pegasusClientInterface = pInterface;
    for (int i = 0; i < 6; ++i) {
      Thread oneThread =
          new Thread(
              new Runnable() {
                @Override
                public void run() {
                  int i = 0;
                  while (isRunning) {
                    for (String key : testKeyList) {
                      try {
                        pegasusClientInterface.get("testDeadLock", key.getBytes(), key.getBytes());
                      } catch (PException e) {
                        // ignore exception
                      }
                    }
                    ++i;

                    if (i >= 666) {
                      System.out.println("sub thread is alive......");
                      i = 0;
                    }
                  }
                }
              });
      oneThread.setDaemon(true);
      oneThread.start();
      threadList.add(oneThread);
    }

    MySignalHandler handler = new MySignalHandler();
    Signal.handle(new Signal("TERM"), handler);

    while (isRunning) {
      try {
        Thread.sleep(1500);
      } catch (InterruptedException exp) {
        System.out.println("catch InterruptedException....");
      }
    }

    pInterface.close();
    System.exit(0);
  }
}
