ack编程步骤:
0. 可以设置config.setNumAckers(8);
1. spout 发送消息时（emit）携带消息的id
2. 每个bolt中锚定上个组件（spout或bolt）的tuple，并且向上级节点回执ack或fail

ack和fail的判定：
全部组件都回执ack，则才会调用spout的ack方法
全部组件只要有其中一个组件回执fail、或者有一个处理消息超时，则会调用spout方法中的fail方法对消息进行再次处理

对fail消息的处理：
一般会对消息进行缓存，因为ack和fail处理消息时，是根据消息的id来查找消息的

超时时间的设置：
config.setMessageTimeoutSecs(1);


[注意]
当storm确定标识符为id的消息已经被处理时，会调用ack()方法,并将消息移除发送队列
该方法应该是非阻塞的，其和ack，fail等方法在一个线程里面被调用，如果发生阻塞，
会导致其他方法调用也被阻塞，从引起各种异常。


