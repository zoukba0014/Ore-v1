# ORE PRODUCTER

ORE挖矿架构的生产者

## 功能点

- 监控新的proof 有没有下，下发新任务的proof
- 接受消费者的任务进行新的hash整理
- 将instruction打包
- 发送上链

## 目前逻辑

- 生产者任务直接进入一个链表，当链表头达到5个不同的账号算出来的新hash的时候，打包并且发送上链
- spawn了2个monitor的线程，一个线程去循环监听当前账号新的proof，另外一个监听reward时间，对于reward时间来说一轮是60s，如果你这个reset time大于60s，别发送transaction了，合约会报错的，等着重置就好了

## TODO

和浩爹聊了一下

- 一个是打包方式要两种，一种是instruction打包，尽可能的多，这种情况是在jito验证者没有成为leader的情况下，把total gas量冲上去；第二种是当jito节点成为leader的时候使用jito的bundle，打包transaction（注意这里是transaction）然后给jito小费
- 当你打包发送上链之后，持续监控你这笔交易的状态，如果没有写入区块就重复发送直到写进区块，之前版本是直接扔掉了太浪费了
- 数据结构以及队列还能调整，现在是新的消费者进来，生产者必须重启下发，生产者要改成一直online，新的消费者进来直接下发任务
