# resp实现

## use case

```text
名称：依次写入RESP基本类型到某个文件
范围：子功能
执行者：RESP客户端
主成功场景：
1. 客户端指定文件名，创建相关handler
2. 使用handler写入RESP数据
3. 关闭handler
扩展：
1a. 文件已存在，未指定overwrite，抛出异常
2a. resp数据不合法，抛出异常
```

```text
名称：依次从文件读取出各个RESP基本类型
范围：子功能
执行者：RESP客户端
主成功场景：
1. 客户端指定文件名，返回相关handler
2. 使用handler读取RESP数据，每次一条
3. 关闭handler
扩展：
1a. 文件不存在，抛出异常
2a. RESP格式不正确，抛出异常
```

## ProposalLog结构

```text
round_no
content
paxos np // nullable
paxos na // nullable
paxos va // nullable
agreement // nullable
```
