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

