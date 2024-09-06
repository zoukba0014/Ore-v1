# ORE CONSUMER

ORE 挖矿的消费者

## 功能点

- 最主要是算hash
- 接受任务和提交结果

## 目前逻辑

- 接收到任务之后所有任务都会有线程去算hash，他这个hash不会停止的算出来一个提交一个继续算
- 两个队列通道：一个上传任务结果，一个接受

## TODO

- hash 逻辑要改：尽可能的给多线程在任务里，新的版本思路是启用轮训机制，比如说一开始并行5个任务，当任务一结束之后，kill掉然后跑任务六，在新版本中transaction会一直重发，主要打成功率
- 算力增加GPU算力，现在difficulty越来越高，要突破核心的算力限制
- 增加新consumer连接到producter的时候前期的握手机制，consumer：im ready producter：ok this is your task.类似与这样的前期握手
