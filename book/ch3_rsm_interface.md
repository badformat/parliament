# 复制状态机实现

前面的用户用例没有太多实现细节，要完成功能，我们需要把用例划分为更小粒度的东西。

敏捷开发中，一般使用“用户故事”，代表能给用户带来的价值，用户可能是最终用户、可能是其他系统或模块。

对于边界已经很清晰的模块，我倾向使用“任务” TODO

## usecase


## CRC

```
ReplicatedStateMachine
描述：复制状态机。
属性：
- 复制状态机集群配置的信息。
- 当前最大轮次序号。
- 当前处理中的状态机事件轮次对象。
- 尚未删除的状态机轮次最小轮次序号。
- 本地尚未删除的已达成一致的状态机轮次对象。
- 状态机轮次事件监听对象。
责任：
- 初始化持久化目录。
- 发起某一轮次状态机事件的共识过程。
- 返回某一轮次的轮次对象，若没有，则构建。
- 轮次达成共识后，持久化该轮次信息，更新轮次序号。
- 恢复或启动时，从其他节点状态机更新本地轮次。
- 某轮次达成共识时，通知监听对象。
协作者：
- 复制状态机集群配置对象。
- 状态机轮次对象。
- 状态机事件监听对象。
```

```
Round
描述：复制状态机事件轮次。
属性：
- 该轮次序号。
- 该轮次共识状态。
- 该轮次共识初始值。
- 该轮次共识最终值。
责任：
- 完成该轮共识过程。
- 返回共识状态。
- 返回共识结果。
写作者：
- paxos算法实例对象。
```

# network protocol
request: paxos round prepare n:byte[]
response: paxos round prepare_res ok:boolean n:byte[] na:byte[] va:byte[]

request: paxos round accept n:byte[] value:byte[]
response: paxos round accept_res ok:boolean n:byte[]

request: paxos round decided value:bytes[]
response: paxos round decided_res ok:boolean // TODO optional