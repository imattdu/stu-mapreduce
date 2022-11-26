## stu-mapreduce





### wordcount, wordcount2 基础案例

#### what

统计单词

### writable 序列化

#### what

统计手机号 流量

#### why

可能需要不同机器之间传输对象



Java 的序列化是一个重量级序列化框架（Serializable），一个对象被序列化后，会附带 很多额外的信息（各种校验信息，Header，继承体系等），不便于在网络中高效传输。所以， Hadoop 自己开发了一套序列化机制（Writable）。



#### how

bean 对象序列化

1.必须实现 Writable 接口

2.反序列化时，需要反射调用空参构造函数，所以必须有空参构造

3.重写序列化方法 write

4.重写反序列化方法 readFields

5.注意反序列化的顺序和序列化的顺序完全一致

6.要想把结果显示在文件中，需要重写 toString()，可用"\t"分开，方便后续用。

7.如果需要将自定义的 bean 放在 key 中传输，则还需要实现 Comparable 接口，因为 MapReduce 框中的 Shuffle 过程要求对 key 必须能排序。







### combineTextInputFormat

小文件合并



### partitoner

1.设置reduceTask

```java
 job.setNumReduceTasks(5);
```

2.自定义分区器



（1）如果ReduceTask的数量> getPartition的结果数，则会多产生几个空的输出文件part-r-000xx；
（2）如果1<ReduceTask的数量<getPartition的结果数，则有一部分分区数据无处安放，会Exception；
（3）如 果ReduceTask的数量=1，则不管MapTask端输出多少个分区文件，最终结果都交给这一个
ReduceTask，最终也就只会产生一个结果文件 part-r-00000；



### combiner



1.Combiner是MR程序中Mapper和Reducer之外的一种组件。 

2.Combiner组件的父类就是Reducer。 

3.Combiner和Reducer的区别在于运行的位置 Combiner是在每一个MapTask所在的节点运行; 

4.Combiner的意义就是对每一个MapTask的输出进行局部汇总，以减小网络传输量。 

5.Combiner能够应用的前提是不能影响最终的业务逻辑，而且，Combiner的输出kv 应该跟Reducer的输入kv类型要对应起来





### outputformat

自定义输出目录





### join

reducejoin：不同文件

mapjoin：缓冲文件



处理不同的文件
