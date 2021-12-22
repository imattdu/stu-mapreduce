package com.matt.mapreduce.partitionerwritablecomparable;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * 1. 实现writable接口
 * 2. 重写序列化 反序列化方法
 * 3. 重写空参构造
 * 4. tostring
 *
 * @author matt
 * @create 2021-12-16 0:40
 */
public class FlowBean implements WritableComparable<FlowBean> {

    private long upFlow;
    private long downFlow;
    private long sumFlow;

    public long getUpFlow() {
        return upFlow;
    }

    public void setUpFlow(long upFlow) {
        this.upFlow = upFlow;
    }

    public long getDownFlow() {
        return downFlow;
    }

    public void setDownFlow(long downFlow) {
        this.downFlow = downFlow;
    }

    public long getSumFlow() {
        return sumFlow;
    }

    public void setSumFlow(long sumFlow) {
        this.sumFlow = sumFlow;
    }

    // 重载
    public void setSumFlow() {
        this.sumFlow = this.upFlow + this.downFlow;
    }

    /**
     * 功能：序列化 顺序保持一致
     *
     * @param out
     * @return void
     * @author matt
     * @date 2021/12/16
     */
    @Override
    public void write(DataOutput out) throws IOException {
        out.writeLong(upFlow);
        out.writeLong(downFlow);
        out.writeLong(sumFlow);
    }

    // 反序列化
    @Override
    public void readFields(DataInput in) throws IOException {
        this.upFlow = in.readLong();
        this.downFlow = in.readLong();
        this.sumFlow = in.readLong();
    }

    // 反射使用
    public FlowBean() {


    }


    //1 12  13
    @Override
    public String toString() {
        return upFlow + "\t" + downFlow + "\t" + sumFlow;
    }


    // 排序
    @Override
    public int compareTo(FlowBean flowBean) {

        //return this.age > o.age ? -1 : 1;   降序排列
        // 等价于 return o.age - this.age
        //
        //return this.age > o.age ? 1 : -1;   升序排列
        // 等价于  return this.age - o.age
        // 总流量倒
        // this.age - o.age=-1
        // 这样系统就认为this比o小，所以排在this排在前面，就是升序了
        if (this.getSumFlow() == flowBean.getSumFlow()) {
            return (int) (flowBean.getUpFlow() - this.getUpFlow());
        }
        return (int) (flowBean.getSumFlow() - this.getSumFlow());
    }


}

