# 如何实现一个线性一致的分布式缓存

源码及构建、运行方式请参看[项目主页](https://github.com/z42y/parliament/)和[javadoc参考](./javadoc/index.html)

# 功能及目标

该服务除了实现常见的"put _key_ _value_"、"get _key_"、"del _key0_  \[_key1_ ...\]"操作，
还实现了按范围取值的命令"range _begin_ _end_"，其中begin和end为开始、结束key值，字典序排序。

使用redis的resp协议进行通信，可以直接使用redis-cli连接测试，或者redis客户端库进行操作。

为了容错，允许用户使用多台机器同时提供服务，避免单点故障。当客户端发现某个服务地址不可用时，可以连接到另一个地址进行服务请求。

使用多台机器需要解决[一致性模型](https://zh.wikipedia.org/wiki/%E5%86%85%E5%AD%98%E4%B8%80%E8%87%B4%E6%80%A7%E6%A8%A1%E5%9E%8B)问题。

这个服务保证**线性一致性**，对客户端来说，无论该系统有多少台机器，都和操作一台机器的效果一样。

反过来说，非线性一致的系统，对同一个key，在某段时间出现不同客户端读到不同的值，或者多次读的结果，其顺序与更新的顺序不一致，
比如某key先后被更新为a、b、c，某客户端在某时间三次读取的结果依次为b、c、a，而且另一个客户端三次读取的结果可能依次为c、b、a。

如果满足线性一致性，服务可以安全的用做leader选举、唯一值广播、分布式锁服务等。

例如：

- leader选举：put leader 'my ip'，无论多少个客户端并发写入leader，只会成功一个，其余客户端会返回put失败。
- 用户昵称绑定：put 'a nickname' 'user id', 某个昵称（'a nickname'）在全局只会被赋予唯一一个user id。

# 接收请求
因为redis的网络协议、客户端和库已经非常流行了，项目采用redis协议提供服务。

## JAVA NIO的使用
首先使用NIO接收客户连接，在连接成功的channel上挂载一个[RespReadHandler](./javadoc/io/github/parliament/resp/RespReadHandler.html)类，
使用[RespDecoder](./javadoc/io/github/parliament/resp/RespDecoder.html)类对异步到来的字节报文进行解码，RespReadHandler使用其get方法，判断是否解码完成。

解码完成后，使用[KeyValueEngine](./javadoc/io/github/parliament/kv/KeyValueEngine.html)进行真正的缓存读写处理，

处理完成后，新建一个[RespWriteHandler](./javadoc/io/github/parliament/resp/RespWriteHandler.html)将结果返回给客户端，
接着重新挂载一个RespReadHandler进行下一个请求处理。

重新生成RespWriterHandler和RespReadHandler是为了方便进行GC，当然可以手工管理各种buffer的回收和重利用，这里不做详细设计了。

因为缓存的对象都比较小，[KeyValueEngine](./javadoc/io/github/parliament/kv/KeyValueEngine.html)并没有使用InputStream之类的模式进一步提升异步性能。

## 网络协议的解析构造
[RespDecoder](./javadoc/io/github/parliament/resp/RespDecoder.html)是redis的[RESP协议](https://redis.io/topics/protocol)解码器，
RESP一共有以下几种数据类型：
- SIMPLE_STRING 字符串
- ERROR 错误字符串
- INTEGER 整数
- BULK_STRING 二进制字节组
- ARRAY 数组

除了ARRAY可以包含其他类型和其他ARRAY，处理稍稍麻烦外，其他类型都容易解析。

使用JAVA标准库中ByteBuffer类直接解析协议是比较困难的，因为报文的写入和读取是并发的，不可能等到报文读取完成后，才开始解析，
甚至无法知道报文什么时候结束。

另外，报文处理往往需要"回溯"操作，从之前某个位置重新开始解析。使用ByteBuffer的flip和rewind、reset太底层，抽象层次不够。

所以通过实现自己的[ByteBuf](./javadoc/io/github/parliament/resp/ByteBuf.html)进行报文解析，主要提供了独立的读写index，方便回溯和读写操作分离。
底层使用byte[]保存数据，也可以使用direct allocate的ByteBuffer提升性能，但是ByteBuf的生命周期短、数据量都小，无法体现其优势。

# 解决一致性问题
前面说到服务需要保证线性一致，如果[KeyValueEngine](./javadoc/io/github/parliament/kv/KeyValueEngine.html)直接对key-value进行读写，将会导致不一致的问题。

举个简单的例子：

- 开始时，客户端A和客户端B都连接了服务S1.
- 客户端A的网络出现问题，与S1的连接断开，切换到服务S2，客户端B保持不变。
- 客户端A和B对同一个key进行不同更新。
- 该key在S1和S2出现冲突，违背了一致性保证。

很多key-value服务（如亚马逊的DynamoDB）采用多写+多读（写节点+读节点>节点总数）的方式冲突发现，采用[逻辑时钟](https://en.wikipedia.org/wiki/Logical_clock)解决冲突，保证最终读一致，
对于昵称的例子，仅仅是最终读一致的保证可能导致某个昵称在某一段时间被两个用户同时使用。

保证线性一致性的常见思想是复制状态机。

## 复制状态机
复制状态机思想是将程序看做一个状态机，在某个状态下，对某个确定的输入，程序的下一个状态是确定的。

举个例子，对两个数据库进行完全相同的一系列读写操作后，这两个数据库的数据一定是一样的（当然不能使用如current_timestamp之类的sql函数）。

同样的，在缓存服务中，所有节点的初始状态是一样的，如果所有客户端的读写操作，在所有节点上按相同顺序进行执行，那么任何一个操作完成时，所有节点状态一定是一致的。

## 全序广播（原子广播）及共识
多个客户端会发生并发操作，服务器无法区分这些操作的先后顺序，因为硬件时钟并不可靠，即便存在一个可靠的时钟，网络的延迟也导致操作在不同节点有不同的到达顺序。

同时，节点在收到一个操作请求时，如何确定是否有之前的操作尚未到达？

所以需要对操作分配一个全局递增的编号，确定操作的顺序，同时所有节点对在这个编号的操作达成一致。

这是一个典型的分布式共识问题：对某一个提案的内容达成一致。

协商**每个**编号操作内容的过程，就是**一次**分布式共识达成过程，因此全序广播问题等价为共识问题。

## paxos共识算法
我们使用Paxos共识算法，paxos算法的推导过程就是一个为了得到结果，不断对条件进行约束的过程。

首先，某个节点作为发起者（proposer），对其他节点发起提案，一种直观的方法是：如果接收者（acceptor）同意该提案，返回t，否则返回f。
如所有接收者返回t，则节点a通知所有接收者提案通过，所有节点执行该提案内容，否则节点a通知提案取消。接收者需要满足**约束P1**：

    P1：一个 acceptor 必须接受（accept）第一次收到的提案。
    
显然，违反P1的系统，不会通过任何提案被通过。

以上方案实际类似于[两阶段提交算法](https://zh.wikipedia.org/zh-hans/%E4%BA%8C%E9%98%B6%E6%AE%B5%E6%8F%90%E4%BA%A4)，
该方法最大的问题是存在单点故障，在提案请求阶段，任何一个节点失败，都会导致提案无法通过。
因为两阶段提交是为了保证各个参与者的任务要么都成功，要么都取消，而不是为了保证高可用。

为了高可用（这是使用多个节点的原因！），必须容忍部分节点的网络故障或进程失败。

但如果网络分裂为A和B两个网络，A和B不能互通，一个提案在A得到批准，另一个提案在B网络得到批准，则会出现提案不一致的情况。
解决方案是只批准获得超过一半（大多数）接收者同意的提案。

但是A和B网络可能有重叠，此时某些节点可能会收到两个或两个以上的提案，这些节点必须能够接受这些两个及以上的提案，因为按照约束P1，
这些提案可能是其他节点的第一个提案，已经被接受了。

为了区分提案，需要为提案分配编号，比如发起节点的本地序列号+ip地址。这里不要混淆提案的编号和复制状态机输入的编号，
状态机每个编号（依次递增）的输入内容对应一次共识过程，该共识过程可能存在多个编号（只需保证单调）的提案。

既然接收者需接收多个提案，这就引出**约束P2**：

    P2：一旦一个具有 value v 的提案被批准（chosen），那么被批准（chosen）的更高编号的提案必须具有 value v。

提案被批准，表示至少被一个接收者接受过。所以加强P2，得到约束P2a：

    一旦某个提案值v获得批准，任何接收者接收的更高编号的提案值也是v。
    
因为通信是异步的，一个从休眠或故障恢复的节点，给某个尚未收到任何提案的节点，提交一个更高编号的不同提案v1，按照约束P1，该节点必须接收该提案，
这就违背了约束P2a，所以，与其约束接收者，约束提交者更加方便，对P2a加强约束，得到约束P2b：

    一旦某个提案值v获得批准，任何发起者发起的更高编号的提案值必须是v。

我们来证明P2b如何保证P2。

使用归纳法，假设编号为m（m < n）的提案被选中，且m到n-1的提案值都为v，那么存在一个大多数接收者的集合C接收了m提案，这意味着：

    C中每个接收者都批准了m到n-1其中一个提案，m到n-1的每个被批准的提案其值都是v。

因为任何大多数接收者集合S，和C至少有一个公共接收者，编号为n的提案w被批准，那么只有两种情况：

    1. 存在一个包含大多数接收者的集合S，从未接受过小于n的提案。
    2. w和S中所有已接受的、编号小于n的最大编号提案值相同，即值为v。因为公共接收者需要批准相同的提案值。

这个证明看起来很多余，但是请注意，编号m到n的提案不是按编号先后顺序发起的，这些提案的发起顺序是没有保证的。

编号为n提案的发起者需要知道所有已接受提案中小于n的最大编号提案的值（如果有）。知道已接受的提案是值很简单的，预测未来很难办，
比如：编号为n的提案值，如何知道尚未收到的n-1编号提案的值呢。

与其预测未来，不如让接受者在之后拒绝所有小于n的提案。

由此强化约束P1，得到P1a:
    
    接收者拒绝接受编号比当前已知最大编号n更小的提案。

得到paxos算法过程如下：

- prepare阶段：
    - 发起者选择一个提案编号n并将prepare请求发送给接收者中的一个多数派；
    - 接收者收到prepare消息后，如果提案的编号大于它已经回复的所有prepare消息(回复消息表示接受accept)，
则接收者将自己上次接受的提案回复给发起者，并承诺不再回复小于n的提案；如果没有回复过prepare消息，也承诺不再回复小于n的提案。
- 批准阶段：
    - 当一个发起者收到了多数接收者对prepare的回复后，就进入批准阶段。
 它要向回复prepare请求的接收者发送accept请求，包括编号n和prepare阶段返回的小于n的最大提案的值。
    - 如果accept的提案编号n大于等于接收者已承诺的编号值，接收者就批准这个请求。
    - accept被多数派批准后，发起者再通知所有接收者提案已批准（decided)的消息。
 
# 实现复制状态机
[KeyValueEngine](./javadoc/io/github/parliament/kv/KeyValueEngine.html)收到请求，不会立即执行，
而是交给[ReplicateStateMachine](./javadoc/io/github/parliament/ReplicateStateMachine.html)生成一个新的状态机输入，
并委托ReplicateStateMachine对该输入所在编号的操作达成共识，由ReplicateStateMachine回调KeyValueEngine接口执行，返回结果。

```
Input input = rsm.newState(bytes);
CompletableFuture<Output> future = rsm.submit(input);
return future.thenApply((output) -> {
    try {
        if (!Arrays.equals(input.getUuid(), output.getUuid())) {
            return RespError.withUTF8("共识冲突");
        }
        return RespDecoder.create().decode(output.getContent()).get();
    } catch (Exception e) {
        logger.error("get submit result failed:", e);
        return RespError.withUTF8("get submit result failed:" + e.getClass().getName()
                + ",message:" + e.getMessage());
    }
});
```
如果达成的共识内容不是提交的内容，返回客户端错误，客户端可以决定重试或报错。

这里需要注意，每个客户端的请求需要分配独立的id，以区分相同内容的客户请求，假如有递增命令inc，两个"inc x"请求不加id会达成一次共识，但实际只执行了一次。
这与客户预期不一致，导致bug。如下所示：

```$java
public Input newState(byte[] content) throws DuplicateKeyException {
    return Input.builder().id(next()).uuid(uuid()).content(content).build();
}
```

ReplicateStateMachine可以并发进行多个Paxos共识实例，每个实例递增分配一个编号，所有RSM实例都按照编号顺序，使用后台线程顺序执行所有编号的共识结果。

顺序执行意味着KeyValueEngine无法并发处理已完成共识的各个请求，如果需要提高并发性，需要保证不同机器并发执行的结果一样，这是比较困难的，
需要完善各种锁机制和并发控制，这里不做实现。

进程可能在KeyValueEngine执行数据操作命令过程中失败，或者执行完成，但在返回ReplicateStateMachine前失败，服务需要在恢复时恢复之前的正确状态。
数据库一般需要采用[写前日志](https://en.wikipedia.org/wiki/Write-ahead_logging)技术保证事务可恢复。

本应用的PUT、DEL、GET都是幂等的，重复执行没有问题，只要保证不漏掉命令就行，ReplicateStateMachine的执行日志可以保证这一点，
具体可查看[start](./javadoc/io/github/parliament/ReplicateStateMachine.html#start(io.github.parliament.StateTransfer,java.util.concurrent.Executor))
和[apply](./javadoc/io/github/parliament/ReplicateStateMachine.html#apply())方法、[done](./javadoc/io/github/parliament/ReplicateStateMachine.html#done(int))方法。

ReplicateStateMachine并发提交共识请求给共识服务[Coordinator](./javadoc/io/github/parliament/Coordinator.html)，
Coordinator可以由各种共识算法实现。

# 实现Paxos共识算法
完成一次共识过程的Paxos[伪代码](http://nil.csail.mit.edu/6.824/2015/notes/paxos-code.html)如下：
```
--- Paxos Proposer ---
      	
     1	proposer(v):
     2    while not decided:
     2	    choose n, unique and higher than any n seen so far
     3	    send prepare(n) to all servers including self
     4	    if prepare_ok(n, na, va) from majority:
     5	      v' = va with highest na; choose own v otherwise   
     6	      send accept(n, v') to all
     7	      if accept_ok(n) from majority:
     8	        send decided(v') to all
      	
        
--- Paxos Acceptor ---

     9	acceptor state on each node (persistent):
    10	 np     --- highest prepare seen
    11	 na, va --- highest accept seen
      	
    12	acceptor's prepare(n) handler:
    13	 if n > np
    14	   np = n
    15	   reply prepare_ok(n, na, va)
    16   else
    17     reply prepare_reject
      	
      	
    18	acceptor's accept(n, v) handler:
    19	 if n >= np
    20	   np = n
    21	   na = n
    22	   va = v
    23	   reply accept_ok(n)
    24   else
    25     reply accept_reject
```
[Paxos类](./javadoc/io/github/parliament/paxos/Paxos.html)作为Paxos服务的门面类，提供共识请求、共识结果查询等功能入口。
他为每个共识实例创建相应的发起者（proposer)，同时为本节点和其他节点的发起者创建、管理对应的接收者（acceptor)。

[Proposer](./javadoc/io/github/parliament/paxos/proposer/Proposer.html)为发起者实现，
[LocalAcceptor](./javadoc/io/github/parliament/paxos/acceptor/LocalAcceptor.html)为接收者实现。

各个实例的提案请求可能来自其他节点，所以提供一个网络服务[PaxosServer](./javadoc/io/github/parliament/paxos/server/PaxosServer.html)，
通过参数中的编号区分不同共识过程实例，转发给不同实例的本地接收者处理，然后返回响应。

配套的，提供[SyncProxyAcceptor](./javadoc/io/github/parliament/paxos/client/SyncProxyAcceptor.html)作为远端接收者的本地网络代理，
请求各个PaxosServer完成提案过程。

[InetPeerAcceptors](./javadoc/io/github/parliament/paxos/client/InetPeerAcceptors.html)为SyncProxyAceptor的创建工厂，
使用一个简单的[连接池](./javadoc/io/github/parliament/paxos/client/ConnectionPool.html)为SyncProxyAcceptor提供nio channel实例。

接口参数使用RESP协议编解码，SyncProxyAcceptor使用同步网络API，简化使用逻辑。
如prepare方法的代理：
```
Prepare delegatePrepare(int round, String n) throws IOException {
    synchronized (channel) {
        ByteBuffer request = codec.encodePrepare(round, n);
        while (request.hasRemaining()) {
            channel.write(request);
        }

        return codec.decodePrepare(channel, n);
    }
}
```

上面的伪代码很简单，但是在进程可能异常退出的情况下，是不够完备的。进程被杀死、断电都会导致正在共识协商的进程退出，并在恢复后，出现错误的结果。

举个例子，多数派为某个编号的共识实例通过了一个提案，但是在decide阶段，多数派全部异常退出，只有多数派以外的节点处理了提案结果。
稍后这些多数派又恢复，之前的信息已经丢失，此时又收到同一个共识实例编号的另一个提案，此提案被通过，和未异常退出的节点接受的提案不一致。

所以，在prepare和accept阶段，都需要持久化Acceptor的状态，并在创建实例的Acceptor时，先尝试恢复持久化的状态。
如[LocalAcceptor](./javadoc/io/github/parliament/paxos/acceptor/LocalAcceptor.html)的prepare：
```
@Override
public synchronized Prepare prepare(String n) throws Exception {
    if (np == null || n.compareTo(np) > 0) {
        np = n;
        persistence(); // 保存状态
        return Prepare.ok(n, na, va);
    }
    return Prepare.reject(n);
}
```
[Paxos类](./javadoc/io/github/parliament/paxos/Paxos.html)保存和恢复acceptor的方法分别如下：
```
void persistenceAcceptor(int round, LocalAcceptor acceptor) throws IOException, ExecutionException {
    if (Strings.isNullOrEmpty(acceptor.getNp())) {
        return;
    }
    persistence.put((round + "np").getBytes(), acceptor.getNp().getBytes());
    if (!Strings.isNullOrEmpty(acceptor.getNa())) {
        persistence.put((round + "na").getBytes(), acceptor.getNa().getBytes());
    }
    if (acceptor.getVa() != null) {
        persistence.put((round + "va").getBytes(), acceptor.getVa());
    }

    persistence.put((round + "checksum").getBytes(), checksum(acceptor.getNp(), acceptor.getNa(), acceptor.getVa()));
}

Optional<LocalAcceptor> regainAcceptor(int round) throws IOException, ExecutionException {
    byte[] np = persistence.get((round + "np").getBytes());

    if (np == null) {
        return Optional.empty();
    }
    byte[] na = persistence.get((round + "na").getBytes());
    byte[] va = persistence.get((round + "va").getBytes());


    String nps = new String(np);
    String nas = na == null ? null : new String(na);

    byte[] checksum1 = checksum(nps, nas, va);
    byte[] checksum2 = persistence.get((round + "checksum").getBytes());
    if (Arrays.equals(checksum1, checksum2)) {
        return Optional.of(new LocalAcceptorWithPersistence(round, nps, nas, va));
    }
    deleteAcceptor(round);
    return Optional.empty();
}
```

持久化过程也可能会失败，所以同时计算保存checksum，在恢复时校验，保证数据完整性。如果checksum不正确，说明该进程在该共识过程中异常退出了，
对共识结果并无影响，可以安全删除数据，并[学习](./javadoc/io/github/parliament/ReplicateStateMachine.html#catchUp())其他节点的共识结果赶上进度。

同时，输入一直在增长，需要删除共识服务中已经处理完成的输入，见[forget方法](./javadoc/io/github/parliament/ReplicateStateMachine.html#forget())。

# 持久化
假设所有节点宕机重启，复制状态机可以依次执行所有共识结果中恢复缓存数据，显然这需要一直保留所有共识结果，且恢复过程会非常慢。

所以缓存数据需要单独做持久化，重启只需执行尚未被执行的共识。前面已经提到过了复制状态机基于执行日志的重试机制以及其局限，
下面讲讲对缓存数据进行持久化会遇到的各种细节。

## skip list算法
GET和PUT、DEL、RANGE是典型的有序Map的操作，比如JDK中的[NavigableMap接口](https://docs.oracle.com/javase/9/docs/api/java/util/NavigableMap.html)，
其并发实现[ConcurrentSkipListMap](https://docs.oracle.com/javase/9/docs/api/java/util/concurrent/ConcurrentSkipListMap.html)采用了skip list算法，
本应用也采用该算法。

skip list（跳表）平均查找和插入时间复杂度都是O(log n)，算法说明见[wikipedia](https://zh.wikipedia.org/wiki/%E8%B7%B3%E8%B7%83%E5%88%97%E8%A1%A8)。
该算法通过维护多层链表，依次快速接近查找目标。

演示（来自Wikipedia）：

![skip list](./skiplist.gif)

## skip list的持久化实现
在内存中实现skip list的数据结构非常简单，使用文件接口的持久化实现则需要考虑很多细节，例如：

- 在内存实现中新增一个list node，只需使用new操作分配一个相关对象，文件实现要考虑：
    - 在哪个文件里为node分配存储空间？
    - 在该文件哪个位置开始分配？
    - 存储node的编码格式？即如何表达node的地址、大小、字段边界？
- 在内存实现中更新一个节点，只需获得节点引用，更新其成员对象的内容或成员引用，文件实现则要考虑：
    - 待更新的node在哪个文件？在文件的哪个位置？
    - node内容增多后，如何在文件里扩展？
- 在内存实现中删除一个节点，只需对相关引用变量赋值null，JVM的GC自动回收内存，文件实现则要考虑：
    - 删除后的node空间如何回收？
- 此外，还需考虑文件更新操作的并发安全。

## Page管理
