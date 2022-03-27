* 问题背景

    实现计算一天的uv，采用ContinuousEventTimeTrigger 来3分钟触发一次,taskmanager发生了OOM

* 配置相关

    配置是2个机器，每个2核，slots设置的也是每个2，并行度是4，其他jobmanager和taskmanager的内存是默认配置
    
    
 

* 代码片段

```
inputStream
                .filter(data -> "pv".equals(data.getBehavior()))
                .keyBy(new KeySelector<ShoppingRecords, Tuple2<LocalDate, Long>>() {
                    @Override
                    public Tuple2<LocalDate, Long> getKey(ShoppingRecords value) throws Exception {
                        Instant instant = Instant.ofEpochMilli(value.getTs());
                        return Tuple2.of(
                                LocalDateTime.ofInstant(instant, ZoneId.of("Asia/Shanghai")).toLocalDate(),
                                value.getItemId()
                        );
                    }
                })
                .window(TumblingEventTimeWindows.of(Time.days(1)))
//                .trigger(ContinuousEventTimeTrigger.of(Time.minutes(3)))
//                .evictor(TimeEvictor.of(Time.seconds(0),true))
                .trigger(PurgingTrigger.of(ContinuousEventTimeTrigger.of(Time.minutes(3))))
                .process(new ProcessWindowFunctionBitMap())
//                .addSink(new RedisSink<>(conf, new UvRedisSink()));
                .addSink(new PrintSinkFunction());

```


* 采取的排查步骤

1. 在次运行了一下,jstat采样可以发现有多次fullgc

    jstat -gcutil  <pid> 2000 10

      0.00 100.00   7.22  86.09  92.98  90.04   2166  105.067     0    0.000  105.067
      0.00 100.00   7.22  86.09  92.98  90.04   2166  105.067     0    0.000  105.067
      0.00 100.00   7.22  86.09  92.98  90.04   2166  105.067     0    0.000  105.067
      0.00 100.00   7.22  86.09  92.98  90.04   2166  105.067     0    0.000  105.067
      0.00 100.00   8.25  86.09  92.98  90.04   2166  105.067     0    0.000  105.067

2.配置失败时生成dump文件

env.java.opts: -XX:+UseCMSInitiatingOccupancyOnly -XX:+AlwaysPreTouch -server -XX:+HeapDumpOnOutOfMemoryError -XX:HeapDumpPath=/opt/metaspace.hprof

- 分析dump文件发现占用的还是ContinuousEventTimeTrigger所在的window多

![36900865a1879354baf1c9a385f9f2aa.png](en-resource://database/6392:0)


- 查阅了一些资料及阅读了flink相关的代码部分,ContinuousEventTimeTrigger 触发时仅仅返回了FIRE,不会删除window state
##ContinuousEventTimeTrigger中
```
@Override
public TriggerResult onEventTime(long time, W window, TriggerContext ctx) throws Exception {


   if (time == window.maxTimestamp()){
      return TriggerResult.FIRE;
   }

   ReducingState<Long> fireTimestampState = ctx.getPartitionedState(stateDesc);

   Long fireTimestamp = fireTimestampState.get();

   if (fireTimestamp != null && fireTimestamp == time) {
      fireTimestampState.clear();
      fireTimestampState.add(time + interval);
      ctx.registerEventTimeTimer(time + interval);
      return TriggerResult.FIRE;
   }

   return TriggerResult.CONTINUE;
}

```



在WindowOperator的processElement方法中,仅仅trigger返回Purger才会进行clear

```
windowState.setCurrentNamespace(stateWindow);
windowState.add(element.getValue());


triggerContext.key = key;
triggerContext.window = actualWindow;


TriggerResult triggerResult = triggerContext.onElement(element);


if (triggerResult.isFire()) {
   ACC contents = windowState.get();
   if (contents == null) {
      continue;
   }
   emitWindowContents(actualWindow, contents);
}


if (triggerResult.isPurge()) {
   windowState.clear();
}
registerCleanupTimer(actualWindow);
```

- 于是采取了如下步骤
1.采用了evictor              
.trigger(ContinuousEventTimeTrigger.of(Time.minutes(3)))                
.evictor(TimeEvictor.of(Time.seconds(0),true)) 

2.采用了 .trigger(PurgingTrigger.of(ContinuousEventTimeTrigger.of(Time.minutes(3))))

PurgingTrigger会是的trigger之后返回Fire and Purge,window会清理数据这2种方式依然oom了
jstat 可以看到一只在发生fullgc， checkpoint的大小一直在增大 大概到300m最大
此处疑惑的是PurgingTrigger应该会进行清理,但为什么还是OOM了

- 继续分析dumpmat分析dump 内存占用较大的是 

org.apache.flink.runtime.state.heap.CopyOnWriteStateMap$StateMapEntry和 org.apache.flink.streaming.api.operators.TimerHeapInternalTimer.
似乎state和定时器都未被清理![1708c20e01bff6ddee2070be43cd1bf3.png](en-resource://database/6394:1)

3.此时不知道接下来怎么排查了,在网上提问,对方说要看一下gclog,对方提到本身taskmanager是默认配置的内存,TM Task Heap只有300多M.可能还来不及清楚就OOM掉了

- 生成gclog,配置文件配置
- 
env.java.opts: -Xloggc:/opt/gc.log -XX:+PrintGCApplicationStoppedTime -XX:+PrintGCDetails -XX:-OmitStackTraceInFastThrow -XX:+PrintGCTimeStamps -XX:+PrintGCDateStamps -XX:+UseGCLogFileRotation -XX:NumberOfGCLogFiles=20 -XX:GCLogFileSize=20M -XX:+PrintPromotionFailure -XX:+PrintGCCause

参考https://zhuanlan.zhihu.com/p/352779662,用GcView查看

![9f0059ae36d10d2b63a24082488eba76.png](en-resource://database/6396:1)

用gcviewr分析了一下gclog, Gcviewr看显示fullgc后最大剩下300M左右,此处帖子提到heap要放大道300M的 3-5倍,于是把TM heap提升到了1GB

flink-conf.yml配置#taskmanager.memory.process.size: 1728m
#taskmanager.memory.process.size: 2240m
taskmanager.memory.process.size: 3128m

至此就未发生OOM了

- 参考资料
GcViewr 相关
https://zhuanlan.zhihu.com/p/352779662https://www.modb.pro/db/173557

flink内存模型
https://www.modb.pro/db/107216
MAT使用https://www.cnblogs.com/zhujiqian/p/14928210.html

火焰图相关
https://blog.csdn.net/weixin_43975771/article/details/118544573https://www.ruanyifeng.com/blog/2017/09/flame-graph.html


