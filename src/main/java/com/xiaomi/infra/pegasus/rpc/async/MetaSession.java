/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package com.xiaomi.infra.pegasus.rpc.async;

import com.google.common.net.InetAddresses;
import com.xiaomi.infra.pegasus.base.error_code.error_types;
import com.xiaomi.infra.pegasus.base.rpc_address;
import com.xiaomi.infra.pegasus.operator.client_operator;
import com.xiaomi.infra.pegasus.operator.create_app_operator;
import com.xiaomi.infra.pegasus.operator.query_cfg_operator;
import com.xiaomi.infra.pegasus.replication.partition_configuration;
import io.netty.channel.EventLoopGroup;
import java.util.*;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.FutureTask;
import java.util.concurrent.TimeUnit;

public class MetaSession extends HostNameResolver {
  public MetaSession(
      ClusterManager manager,
      String[] addrList,
      int eachQueryTimeoutInMills,
      int defaultMaxQueryCount,
      EventLoopGroup g)
      throws IllegalArgumentException {
    clusterManager = manager;
    metaList = new ArrayList<ReplicaSession>();

    if (addrList.length == 1 && !InetAddresses.isInetAddress(addrList[0])) {
      // if the given string is not a valid ip address,
      // then take it as a hostname for a try.
      resolveHost(addrList[0]);
      if (!metaList.isEmpty()) {
        hostPort = addrList[0];
      }
    } else {
      for (String addr : addrList) {
        rpc_address rpcAddr = new rpc_address();
        if (rpcAddr.fromString(addr)) {
          logger.info("add {} as meta server", addr);
          metaList.add(clusterManager.getReplicaSession(rpcAddr));
        } else {
          logger.error("invalid address {}", addr);
        }
      }
    }
    if (metaList.isEmpty()) {
      throw new IllegalArgumentException("no valid meta server address");
    }
    curLeader = 0;

    this.eachQueryTimeoutInMills = eachQueryTimeoutInMills;
    this.defaultMaxQueryCount = defaultMaxQueryCount;
    this.group = g;
  }

  public static error_types getMetaServiceError(client_operator metaQueryOp) {
    if (metaQueryOp.rpc_error.errno != error_types.ERR_OK) return metaQueryOp.rpc_error.errno;

    if (metaQueryOp instanceof query_cfg_operator) {
      return ((query_cfg_operator) (metaQueryOp)).get_response().getErr().errno;
    } else if (metaQueryOp instanceof create_app_operator) {
      return ((create_app_operator) (metaQueryOp)).get_response().getErr().errno;
    } else {
      assert (false);
      return null;
    }
  }

  public static rpc_address getMetaServiceForwardAddress(client_operator metaQueryOp) {
    if (metaQueryOp.rpc_error.errno != error_types.ERR_OK) return null;

    rpc_address addr = null;
    if (metaQueryOp instanceof query_cfg_operator) {
      query_cfg_operator op = (query_cfg_operator) metaQueryOp;
      if (op.get_response().getErr().errno != error_types.ERR_FORWARD_TO_OTHERS) return null;
      java.util.List<partition_configuration> partitions = op.get_response().getPartitions();
      if (partitions == null || partitions.isEmpty()) return null;
      addr = partitions.get(0).getPrimary();
      if (addr == null || addr.isInvalid()) return null;
    }

    return addr;
  }

  public void asyncExecute(
      client_operator op,
      Runnable callbackFunc,
      MetaRequestRoundType roundType,
      int maxQueryCount,
      long timeoutMs) {
    MetaRequestRound round;
    synchronized (this) {
      round =
          new MetaRequestRound(
              op, callbackFunc, metaList.get(curLeader), roundType, maxQueryCount, timeoutMs);
    }
    asyncCall(round);
  }

  private void execute(
      client_operator op, MetaRequestRoundType roundType, int maxQueryCount, long timeoutMs) {
    FutureTask<Void> v =
        new FutureTask<Void>(
            new Callable<Void>() {
              @Override
              public Void call() throws Exception {
                return null;
              }
            });

    asyncExecute(op, v, roundType, maxQueryCount, timeoutMs);

    while (true) {
      try {
        v.get();
        return;
      } catch (InterruptedException e) {
        logger.info("operation {} got interrupt exception: ", op.get_gpid().toString(), e);
      } catch (ExecutionException e) {
        logger.warn(
            "operation {} got execution exception, just return: ", op.get_gpid().toString(), e);
        return;
      }
    }
  }

  public final void executeWithTimeout(client_operator op, long timeoutMs) {
    this.execute(op, MetaRequestRoundType.ROUND_WITH_TIMEOUT, 0, timeoutMs);
  }

  public final void executeWithMaxExecuteCount(client_operator op, int maxExecuteCount) {
    this.execute(op, MetaRequestRoundType.ROUND_WITH_MAX_EXECUTE_COUNT, maxExecuteCount, 0);
  }

  public final void closeSession() {
    for (ReplicaSession rs : metaList) {
      rs.closeSession();
    }
  }

  private void asyncCall(final MetaRequestRound round) {
    long timeoutMs = 0;
    switch (round.roundType) {
      case ROUND_WITH_MAX_EXECUTE_COUNT:
        timeoutMs = eachQueryTimeoutInMills;
        break;
      case ROUND_WITH_TIMEOUT:
        timeoutMs = round.endRoundTime - System.currentTimeMillis();
        if (timeoutMs < 0) {
          timeoutMs = 8;
        }
        break;
      default:
        assert (false);
    }

    round.lastSession.asyncSend(
        round.op,
        new Runnable() {
          @Override
          public void run() {
            onFinishMetaRequest(round);
          }
        },
        timeoutMs,
        false);
  }

  void onFinishMetaRequest(final MetaRequestRound round) {
    client_operator op = round.op;

    boolean needDelay = false;
    boolean needSwitchLeader = false;
    rpc_address forwardAddress = null;

    if (MetaRequestRoundType.ROUND_WITH_MAX_EXECUTE_COUNT == round.roundType) {
      --round.maxExecuteCount;
    }

    error_types metaError = error_types.ERR_UNKNOWN;
    if (op.rpc_error.errno == error_types.ERR_OK) {
      metaError = getMetaServiceError(op);

      // do real operator's specific logic
      if (op instanceof create_app_operator) {
        if (metaError == error_types.ERR_BUSY_CREATING) {
          ((create_app_operator) (op)).get_response().getErr().errno = error_types.ERR_OK;
          round.callbackFunc.run();
          return;
        }
      } else if (op instanceof query_cfg_operator) {
        // do nothing now
      }

      // do operator's common logic
      if (metaError == error_types.ERR_SERVICE_NOT_ACTIVE) {
        needDelay = true;
      } else if (metaError == error_types.ERR_FORWARD_TO_OTHERS) {
        needSwitchLeader = true;
        forwardAddress = getMetaServiceForwardAddress(op);
      } else {
        round.callbackFunc.run();
        return;
      }

    } else if (op.rpc_error.errno == error_types.ERR_SESSION_RESET
        || op.rpc_error.errno == error_types.ERR_TIMEOUT) {
      needSwitchLeader = true;
    } else {
      logger.error("unknown error: {}", op.rpc_error.errno.toString());
      round.callbackFunc.run();
      return;
    }

    logger.info(
        "query meta got error, rpc error({}), meta error({}), forward address({}), current leader({}), "
            + "remain retry count({}), need switch leader({}), need delay({})",
        op.rpc_error.errno.toString(),
        metaError.toString(),
        forwardAddress,
        round.lastSession.name(),
        round.maxExecuteCount,
        needSwitchLeader,
        needDelay);
    synchronized (this) {
      if (needSwitchLeader) {
        if (forwardAddress != null && !forwardAddress.isInvalid()) {
          boolean found = false;
          for (int i = 0; i < metaList.size(); i++) {
            if (metaList.get(i).getAddress().equals(forwardAddress)) {
              curLeader = i;
              found = true;
              break;
            }
          }
          if (!found) {
            logger.info("add forward address {} as meta server", forwardAddress);
            metaList.add(clusterManager.getReplicaSession(forwardAddress));
            curLeader = metaList.size() - 1;
          }
        } else if (metaList.get(curLeader) == round.lastSession) {
          curLeader = (curLeader + 1) % metaList.size();
          // try refresh the meta list from DNS
          // maxResolveCount and "maxQueryCount refresh" is necessary:
          // for example, maxQueryCount=5, the first error metalist size = 3, when trigger dns
          // refresh, the "maxQueryCount" may change to 2, the client may can't choose the right
          // leader when the new metaList size > 2 after retry 2 time. but if the "maxQueryCount"
          // refresh, the retry will not stop if no maxResolveCount when the meta is error.
          if (curLeader == 0 && hostPort != null && round.maxResolveCount != 0) {
            resolveHost(hostPort);
            round.maxResolveCount--;
            round.maxExecuteCount = metaList.size();
          }
        }
      }
      round.lastSession = metaList.get(curLeader);
    }

    switch (round.roundType) {
      case ROUND_WITH_MAX_EXECUTE_COUNT:
        if (round.maxExecuteCount <= 0) {
          round.callbackFunc.run();
          return;
        }
        break;
      case ROUND_WITH_TIMEOUT:
        if (round.endRoundTime <= System.currentTimeMillis()) {
          round.callbackFunc.run();
          return;
        }
        break;
      default:
        assert (false);
    }

    retryQueryMeta(round, needDelay);
  }

  void retryQueryMeta(final MetaRequestRound round, boolean needDelay) {
    long delay = 0;
    if (needDelay) {
      switch (round.roundType) {
        case ROUND_WITH_MAX_EXECUTE_COUNT:
          delay = 1000;
          break;
        case ROUND_WITH_TIMEOUT:
          long remainMs = round.endRoundTime - System.currentTimeMillis();
          if (remainMs < 0) {
            remainMs = 8;
          }
          delay = remainMs / 3;
          break;
        default:
          assert (false);
      }
    }

    group.schedule(
        new Runnable() {
          @Override
          public void run() {
            asyncCall(round);
          }
        },
        delay,
        TimeUnit.MILLISECONDS);
  }

  enum MetaRequestRoundType {
    ROUND_WITH_TIMEOUT,
    ROUND_WITH_MAX_EXECUTE_COUNT,
  }

  static final class MetaRequestRound {
    public int maxResolveCount = 2;

    public client_operator op;
    public Runnable callbackFunc;

    public ReplicaSession lastSession;

    public MetaRequestRoundType roundType;

    public int maxExecuteCount = 0;

    public long endRoundTime;
    public long timeoutMs;

    private MetaRequestRound(client_operator o, Runnable r, ReplicaSession l) {
      op = o;
      callbackFunc = r;
      lastSession = l;
    }

    public MetaRequestRound(
        client_operator o,
        Runnable r,
        ReplicaSession l,
        MetaRequestRoundType roundType,
        int q,
        long tm) {
      this(o, r, l);
      this.roundType = roundType;
      this.maxExecuteCount = q;
      this.timeoutMs = tm;
      this.endRoundTime = System.currentTimeMillis() + tm;
    }

    public MetaRequestRound(client_operator o, Runnable r, ReplicaSession l, int q) {
      this(o, r, l, MetaRequestRoundType.ROUND_WITH_MAX_EXECUTE_COUNT, q, 0);
    }
  }

  /*
   * Resolves hostname:port into a set of ip addresses.
   */
  void resolveHost(String hostPort) throws IllegalArgumentException {
    rpc_address[] addrs = resolve(hostPort);
    if (addrs == null) {
      logger.error("failed to resolve address \"{}\" into ip addresses", hostPort);
      return;
    }

    Set<rpc_address> newSet = new TreeSet<rpc_address>(Arrays.asList(addrs));
    Set<rpc_address> oldSet = new TreeSet<rpc_address>();
    for (ReplicaSession meta : metaList) {
      oldSet.add(meta.getAddress());
    }

    // fast path: do nothing if meta list is unchanged.
    if (newSet.equals(oldSet)) {
      return;
    }

    // removed metas
    Set<rpc_address> removed = new HashSet<rpc_address>(oldSet);
    removed.removeAll(newSet);
    for (rpc_address addr : removed) {
      logger.info("meta server {} was removed", addr);
      for (int i = 0; i < metaList.size(); i++) {
        if (metaList.get(i).getAddress().equals(addr)) {
          ReplicaSession session = metaList.remove(i);
          session.closeSession();
        }
      }
    }

    // newly added metas
    Set<rpc_address> added = new HashSet<rpc_address>(newSet);
    added.removeAll(oldSet);
    for (rpc_address addr : added) {
      metaList.add(clusterManager.getReplicaSession(addr));
      logger.info("add {} as meta server", addr);
    }
  }

  // Only for test.
  List<ReplicaSession> getMetaList() {
    return metaList;
  }

  private ClusterManager clusterManager;
  private List<ReplicaSession> metaList;
  private int curLeader;
  private int eachQueryTimeoutInMills;
  private int defaultMaxQueryCount;
  private EventLoopGroup group;
  private String hostPort;

  private static final org.slf4j.Logger logger =
      org.slf4j.LoggerFactory.getLogger(MetaSession.class);
}
