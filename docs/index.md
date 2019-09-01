# 自制分布式缓存服务：设计及实现

源码及构建请参看[github项目主页](https://github.com/z42y/parliament/)和[javadoc参考](./javadoc/index.html)

# 功能及目标

该服务除了实现常见的"put _key_ _value_"、"get _key_"、"del _key0_  \[_key1_ ...\]"操作，
还实现了按范围取值的命令"range _begin_ _end_"，其中begin和end为开始、结束key值，字典序排序。

使用redis的resp协议进行通信，可以直接使用redis-cli连接测试，或者redis客户端库进行操作。

为了容错，允许用户使用多台机器同时提供服务，避免单点故障。

在多台机器间进行数据读写，需要解决[一致性模型](https://zh.wikipedia.org/wiki/%E5%86%85%E5%AD%98%E4%B8%80%E8%87%B4%E6%80%A7%E6%A8%A1%E5%9E%8B)问题。

该服务提供**线性一致性**，即对客户端来说，无论该系统有多少台机器，都和操作一台机器的效果一样。

反过来说，非线性一致的系统，对同一个key，在某段时间出现不同客户端读到不同的值，或者多次读的结果，其顺序与更新的顺序不一致，
比如某key先后被更新为a、b、c，某客户端在某时间三次读取的结果依次为b、c、a，而且另一个客户端三次读取的结果可能依次为c、b、a。

如果满足线下一致性，这个服务可以做leader选举、唯一值广播、分布式锁服务等。

例如：

- leader选举：put leader 'my ip'，无论多少个客户端并发写入leader，只会成功一个，其余客户端会返回put失败。
- 用户昵称绑定：put 'a nickname' 'user id', 某个昵称（'a nickname'）在全局只会被赋予唯一一个user id。

# 接收请求
## JAVA NIO的使用
## 网络协议的解析构造

# 处理请求
## 线性一致性及原子广播
## 复制状态机
## paxos协议及实现

# 持久化
## SkipList算法
## Page管理


