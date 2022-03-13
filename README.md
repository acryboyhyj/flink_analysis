# flink_anlysis

#### 主要内容
- Flink一些API的使用,对Kafaka,Redis,ck等组件的对接使用
- Flink消费Kafka数据，逻辑处理，写入到ClickHouse
- 作业配置通过的ParamTool读取

#### 数据来源
来自阿里云天池数据集，https://tianchi.aliyun.com/dataset/dataDetail?dataId=649

可以用flink_datagen仓库的程序灌入数据到kafaka
java -classpath {jar.path} myflink.SourceGenerator --input {inputfile.path} --output kafka localhost:9092 --speedup 2000




