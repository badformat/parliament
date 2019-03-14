# paxos复制状态机用例
## 用例
### 用例名称：使用paxos复制状态机接口完成状态复制
执行者：使用paxos复制状态机库完成功能的软件应用

前置条件：
- 已有2n+1台运行paxos复制状态机（RSM）的节点
- 已知各RSM的地址和端口
- 应用通过各个RSM地址及端口创建了一个RSM实例

主流程：
1. 使用RSM实例发起提案，提案内容为字节数组。
2. RSM实例返回该提案的轮次号（注意，不是paxos提案编号），表示该提案作为状态机输入事件，在事件流中的次序。
3. 调用RSM接口向其他节点RSM发起共识开始请求。
4. 应用使用RSM查询本轮提案的进行状态。
5. 如本轮状态已通过，可以查询最终通过提案的内容。客户端可以通过比较查看是否是自己的提案内容通过了。

扩展流程：
- 2a 最后通过的提案内容可能会和本节点应用提出的提案内容不一样。
- 2b 如果该轮次号的提案已经在其他节点为完结状态，其他节点应该返回提案内容和其所知的最大轮次号，本节点查询其他节点，追上其进度后，在重复步骤1。
- 3a 如果该节点在此步失败，且没有任何其他节点收到该请求，和没有提交这个提案一样，客户端业务保证重试流程的正确性。

后置条件：
- 该轮提案状态一定能进入已完成的状态。
- 该轮提案在各个状态机中内容一致，并且被持久化。

### paxos伪代码
TODO：需要更新，不能使用mit代码
```
proposer(v):
  while not decided:
    choose n, unique and higher than any n seen so far
    send prepare(n) to all servers including self
    if prepare_ok(n, n_a, v_a) from majority:
      v' = v_a with highest n_a; choose own v otherwise
      send accept(n, v') to all
      if accept_ok(n) from majority:
        send decided(v') to all

acceptor's state:
  n_p (highest prepare seen)
  n_a, v_a (highest accept seen)

acceptor's prepare(n) handler:
  if n > n_p
    n_p = n
    reply prepare_ok(n, n_a, v_a)
  else
    reply prepare_reject

acceptor's accept(n, v) handler:
  if n >= n_p
    n_p = n
    n_a = n
    v_a = v
    reply accept_ok(n)
  else
    reply accept_reject
```

### 用例名称：paxos实例发起paxos提案
执行者：RSM

前置条件：RSM发起轮次为n的提案请求。

主流程：
1. RSM获取本地当前最大轮次号。
2. RSM向所有其他远端RSM使用该轮次号发起共识开始请求。
3. RSM生成本地paxos proposer及accepor对象，生成远端节点的acceptor代理对象，并将所有acceptor与本地proposer关联。
4. 使用proposer发起propose流程，为了不阻塞RSM，这一步在线程池中运行。
5. propose成功后，持久化通过的提案值，并通知RSM。
6. RSM将本地轮次加1并持久化。
7. RSM通知应用本轮通过的提案值。

扩展流程：
- 2a 如有远端已完成大于等于n的轮次，则返回其最大轮次号m，本地RSM请求其他节点返回从n到m轮次的已通过的paxos实例信息，并持久化到本地。从步骤1重试。
- 3a 进入propose，持久化本轮轮次及自己的提案值。
- 3b proposer收到多数accept_ok后，持久化通过的提案值。
- 3c 如在propose过程中宕机，则恢复后，RSM需检查本轮状态，为非通过状态，从步骤1重试。

后置条件：
- paxos实例最终达成共识。
- 本轮paxos实例信息持久化成功。

### 用例名称：paxos实例接收paxos提案
执行者：RSM

前置条件：收到其他节点的轮次号为n的提案请求。

主流程：
1. 本地RSM检查到n比本地本地已通过的最大轮次号m小，返回该轮次号m。否则进入步骤2。
2. 本地RSM检查轮次为n的paxos算法实例是否存在，不存在则先尝试读取可能存在的本轮持久化信息，恢复paxos算法实例，否则新建一个paxos实例，进入paxos共识算法流程。
3. 收到prepare消息，如n_p被更新后，持久化paxos实例。
4. 收到accept消息后，在返回accept_ok前，持久化paxos实例。

后置条件：
- paxos实例最终达成共识。
- 本轮paxos实例信息持久化成功。

## 领域概念抽象
- Replicated State Machine
- Paxos proposer
- Paxos acceptor
  - 本地acceptor
  - 远端acceptor的代理

一个应用可以维持多个replicated state machine，每个RSM需要持久化一些信息，如当前所知最大轮次号以及各轮paxos算法实例的信息，同时需要知道参与各方的ip地址和端口。

为保持简单，先使用构造函数创建RSM。

paxos proposer在RSM应用中，由RSM创建，以便对外隐藏算法细节，但是为了单独测试及其他形式的使用，可以提供一个proposer公开的创建接口，构造参数为参与投票的各个acceptor。

本地acceptor也类似，比较麻烦的是远端acceptor的本地代理对象，其责任是转发proposer的请求到远端acceptor实例，并返回结果，如何转发并不是paxos算法中acceptor的职责。

转发的实现应该由使用paxos算法的具体应用来决定，不同的paxos算法应用可能有不同的acceptor代理实现。在RSM应用中，就应该由RSM来决定。所以，acceptor代理对象除了实现acceptor的接口，应该还有一个转发组件，该组件负责组装、发送、接收数据。

我们在实现paxos算法流程时，不应该考虑转发细节。

我们先实现paxos算法流程，因为目前我们对此最为熟悉。

TODO：细化

新建类时，使用CRC卡是个不错的手段，明确每个类的职责和相关协作者。

```
proposer类
描述：paxos共识协议中提议者的角色
属性：
- value：自己的提案
- acceptedValue：最终通过的提案
- decided：boolean，是否已通过最终提案
责任：
- 实现提案流程
- 查看提案状态
协作者：
- acceptor
- prepare阶段响应对象
- accept阶段响应对象
```

```
acceptor类
描述：paxos共识协议中接收者的角色
属性：
- n_p：当前已知最大提案编号
- n_a：已接收的提案编号
- v_a：已接收的提案值
责任：
- 实现提案prepare阶段功能
- 查看提案accept阶段功能
协作者：
- prepare阶段响应对象
- accept阶段响应对象
```