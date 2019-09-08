# 自制分布式缓存服务：设计及实现

源码及构建请参看[github项目主页](https://github.com/z42y/parliament/)和[javadoc参考](./javadoc/index.html)

# 功能及目标

该服务除了实现常见的"put _key_ _value_"、"get _key_"、"del _key0_  \[_key1_ ...\]"操作，
还实现了按范围取值的命令"range _begin_ _end_"，其中begin和end为开始、结束key值，字典序排序。

使用redis的resp协议进行通信，可以直接使用redis-cli连接测试，或者redis客户端库进行操作。

为了容错，允许用户使用多台机器同时提供服务，避免单点故障。

使用多台机器需要解决[一致性模型](https://zh.wikipedia.org/wiki/%E5%86%85%E5%AD%98%E4%B8%80%E8%87%B4%E6%80%A7%E6%A8%A1%E5%9E%8B)问题。

这个服务保证**线性一致性**，对客户端来说，无论该系统有多少台机器，都和操作一台机器的效果一样。

反过来说，非线性一致的系统，对同一个key，在某段时间出现不同客户端读到不同的值，或者多次读的结果，其顺序与更新的顺序不一致，
比如某key先后被更新为a、b、c，某客户端在某时间三次读取的结果依次为b、c、a，而且另一个客户端三次读取的结果可能依次为c、b、a。

如果满足线下一致性，服务可以安全的用做leader选举、唯一值广播、分布式锁服务等。

例如：

- leader选举：put leader 'my ip'，无论多少个客户端并发写入leader，只会成功一个，其余客户端会返回put失败。
- 用户昵称绑定：put 'a nickname' 'user id', 某个昵称（'a nickname'）在全局只会被赋予唯一一个user id。

# 接收请求
因为redis的网络协议、客户端和库已经非常流行了，项目采用redis协议提供服务。

## JAVA NIO的使用
首先使用NIO接收客户连接，在连接成功的channel上挂载一个[RespReadHandler](./javadoc/io/github/parliament/resp/RespReadHandler.html)类，
使用[RespDecoder](./javadoc/io/github/parliament/resp/RespDecoder.html)类对异步到来的字节报文进行解码，RespReadHandler使用其get方法，判断是否解码完成。

解码完成后，使用[KeyValueEngine](./javadoc/io/github/parliament/kv/class-use/KeyValueEngine.html)进行真正的缓存读写处理，

处理完成后，新建一个[RespWriteHandler](./javadoc/io/github/parliament/resp/RespWriteHandler.html)将结果返回给客户端，
接着重新挂载一个RespReadHandler进行下一个请求处理。

重新生成RespWriterHandler和RespReadHandler是为了方便进行GC，当然可以手工管理各种buffer的回收和重利用，这里不做详细设计了。

因为缓存的对象都比较小，[KeyValueEngine](./javadoc/io/github/parliament/kv/class-use/KeyValueEngine.html)并没有使用InputStream之类的模式进一步提升异步性能。

## 网络协议的解析构造
[RespDecoder](./javadoc/io/github/parliament/resp/RespDecoder.html)是redis的[RESP协议](https://redis.io/topics/protocol)解码器，
RESP一共有四种数据类型：
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
底层使用byte[]保存数据，也可以使用Direct allocate的ByteBuffer提升性能，但是ByteBuf的生命周期短、数据量都小，无法体现其优势。

# 处理请求
## 线性一致性及原子广播
## 复制状态机
## paxos协议及实现

# 持久化
## SkipList算法
## Page管理
