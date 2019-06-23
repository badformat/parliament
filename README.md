# 简介

使用java实现的分布式key-value服务器。

# 目标

这是我自己实现的一个**分布式**key-value服务的过程文档及代码。

**不承诺任何SLA，生产环境使用，请自担风险**。

分布式意味着只要有一半部署的机器可以工作，该key-value服务就可以**正确的**完成工作。

项目没有使用第三方网络库、分布式共识协议库、存储实现，只使用了jdk9的标准库和lombok代码生成工具、google guava库、junit、mockito等库。

这样，我就可以将网络编程、网络协议解析、异步网络服务端及客户端、同步网络客户端、共识协议的实现、存储实现全部体验一遍。

同时，在可靠性和可用性上，会*尽量*以生产环境的分布式系统要求来实现，这意味着要实现高并发，可以应对网络异常、硬件异常、系统异常。

由于精力原因，我不会实现相关运维工具。

详情见编写中的文档：[如何从头实现一个分布式key-vaule服务](https://z42y.github.io/parliament/)。

# 进展
## DONE
- redis二进制协议resp解析器。
- 基于redis协议的kv服务器。
- kv服务器之间使用的paxos共识服务。
- 基本的持久化存储服务。

## TODO
- 基于btree的持久化服务。

# 构建
- maven
- jdk9+

如果你要使用IDE查看代码，请安装lombok相关[插件](https://projectlombok.org/setup/overview)。

# 使用举例
目前已经可以简单测试使用，但是持久化实现还未完成。

## 例子
在target编译目录下，启动服务器1：

```
java -Dkv="127.0.0.1:7000" -Dme="127.0.0.1:8001" -Dpeers="127.0.0.1:8000,127.0.0.1:8001,127.0.0.1:8002" -Ddir="./db7000" -cp .\dependencies\ -jar .\parliament-1.0-SNAPSHOT.jar
```

服务器2:
```
java -Dkv="127.0.0.1:7001" -Dme="127.0.0.1:8002" -Dpeers="127.0.0.1:8000,127.0.0.1:8001,127.0.0.1:8002" -Ddir="./db70001" -cp .\dependencies\ -jar .\parliament-1.0-SNAPSHOT.jar
```

使用redis-cli客户端，连接各个服务:
```
redis-cli 127.0.0.1:7001
```

目前实现了get\put\del命令：
```
put a A
get a
del a
```