# 简介

parliament是一个带持久化的分布式缓存服务器，基于JDK 9+。

**业余爱好项目，生产环境使用，请自担风险**。

详情见文档：[自制分布式缓存服务：设计及实现](https://z42y.github.io/parliament/)。

版权申明：

    This program is free software: you can redistribute it and/or modify
    it under the terms of the GNU General Public License as published by
    the Free Software Foundation, either version 3 of the License, or
    (at your option) any later version.

    This program is distributed in the hope that it will be useful,
    but WITHOUT ANY WARRANTY; without even the implied warranty of
    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
    GNU General Public License for more details.

    You should have received a copy of the GNU General Public License
    along with this program.  If not, see <https://www.gnu.org/licenses/>.
    
# 构建
- Apache Maven
- JDK 9+

如果你要使用IDE查看代码，请安装lombok相关[插件](https://projectlombok.org/setup/overview)。

# 运行例子
在编译结果目录（target）下，启动端口为7000的服务器1：

```
java -Dkv="127.0.0.1:7000" -Dme="127.0.0.1:8000" -Dpeers="127.0.0.1:8000,127.0.0.1:8001" -Ddir="./db7000" -cp .\dependencies\ -jar .\parliament-1.0-SNAPSHOT.jar
```
参数说明：
- kv：缓存服务进程的端口，供redis客户端连接。
- me：本服务Paxos共识参与者的地址和端口，目前只支持ip v4地址格式。
- peers：所有Paxos共识参与者的地址列表，用逗号分割。
- dir：持久化数据读写目录，注意同一台机器的不同进程要避免目录冲突。

使用java的标准参数“-cp”指定第三方classpath，默认在编译目录target的dependencies目录里。

接着启动7001端口的服务器2，其Paxos共识参与者端口为8001:
```
java -Dkv="127.0.0.1:7001" -Dme="127.0.0.1:8001" -Dpeers="127.0.0.1:8000,127.0.0.1:8001" -Ddir="./db70001" -cp .\dependencies\ -jar .\parliament-1.0-SNAPSHOT.jar
```

使用redis-cli客户端，连接以上某个服务:
```
redis-cli 127.0.0.1:7001
```

目前实现了get\put\del\range（按key的范围查询列表）命令：
```
put a A
get a
del a
range a z
```
