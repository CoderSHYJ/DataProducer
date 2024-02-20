使用方式：运行SimpleKafka、SimpleClient、SimpleFlume、SimpleSpark

SimpleKafka：

提供消息队列服务，当前默认端口8082

SimpleClient

创建两个消息队列，循环读取最终数据

数据发送到http://localhost:8080/emit_dynamic_act（待测试）

SimpleFlume

启动后循环生成数据，可指定随机数据范围

SimpleSpark

在消息队列之间处理数据，预留处理接口，目前直接搬运数据