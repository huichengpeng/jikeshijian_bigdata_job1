## 1. 运行环境

搭建一个纯容器版本的伪分布式集群用于mapreduce作业的运行

```shell
#拉取镜像
docker pull sequenceiq/hadoop-docker

# 启动容器
docker run -p 50070:50070 -p 9000:9000 -p 8088:8088  -it sequenceiq/hadoop-docker /etc/bootstrap.sh -bash
```

![img](https://cdn.nlark.com/yuque/0/2022/png/2981563/1647529475074-7bb7a542-deca-4295-959e-2e329dca853a.png)

测试集群安装情况

```shell
jps
```

![img](https://cdn.nlark.com/yuque/0/2022/png/2981563/1647529606480-85b8a73c-306c-427b-ba6d-78329ef28908.png)

http://localhost:50070/

UI查看集群状态

![img](https://cdn.nlark.com/yuque/0/2022/png/2981563/1647529651926-75433d77-4bad-4f52-95c1-f2d5923a2f91.png)

## 2. mapreducer作业



### 2.1 map class

```java
package com.afa.mapreduce.trafficstatistics;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class TrafficStatisticsMapper extends Mapper<LongWritable, Text, Text, PhoneTraffic> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] lines = value.toString().split("\t");
        //取第二个手机号
        String phone = lines[1];
        try {
            long up = Long.parseLong(lines[8]);
            long down = Long.parseLong(lines[9]);
            context.write(new Text(phone), new PhoneTraffic(up, down, up + down));
        } catch (NumberFormatException e) {
            System.err.println("parserLong failed" + e.getMessage());
        }
    }
}
```



### 2.2 reduce class

```java
package com.afa.mapreduce.trafficstatistics;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;


import java.io.IOException;

public class TrafficStatisticsReducer extends Reducer<Text, PhoneTraffic, Text, PhoneTraffic> {
    public void reduce(Text key, Iterable<PhoneTraffic> values, Context context) throws IOException,
            InterruptedException {
        long totalUp = 0;
        long totalDown = 0;
        long totalTraffic = 0;
        for (PhoneTraffic val : values) {
            totalUp += val.getUp();
            totalDown += val.getDown();
            totalTraffic += val.getSum();
        }
        context.write(key, new PhoneTraffic(totalUp, totalDown, totalTraffic));
    }
}
```



### 2.3 自定义输出类型 

```java
package com.afa.mapreduce.trafficstatistics;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class PhoneTraffic implements Writable {
    private long up;
    private long down;
    private long sum;
    public PhoneTraffic() {
    }
    public PhoneTraffic(long up, long down, long sum) {
        this.up = up;
        this.down = down;
        this.sum = sum;
    }

    long getUp() {
        return up;
    }

    long getDown() {
        return down;
    }

    long getSum() {
        return sum;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeLong(up);
        dataOutput.writeLong(down);
        dataOutput.writeLong(sum);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        this.up = dataInput.readLong();
        this.down = dataInput.readLong();
        this.sum = dataInput.readLong();
    }

    @Override
    public String toString(){
        return this.up + "\t" + this.down + "\t" + this.sum;
    }
}
```



### 2.4 driver 主类

```java
package com.afa.mapreduce.trafficstatistics;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class TrafficStatisticsDriver {
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        System.out.println("极客时间-作业1 阿发-G20220735020087");
        System.out.println("input:" + args[1]);
        System.out.println("outout:" + args[2]);
        Job job = Job.getInstance(conf);
        job.setJarByClass(TrafficStatisticsDriver.class);
        job.setMapperClass(TrafficStatisticsMapper.class);
        job.setCombinerClass(TrafficStatisticsReducer.class);
        job.setReducerClass(TrafficStatisticsReducer.class);

        //设置map输出的数据类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(PhoneTraffic.class);

        job.setNumReduceTasks(1);

        FileInputFormat.setInputPaths(job, new Path(args[1]));
        FileOutputFormat.setOutputPath(job, new Path(args[2]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
```





### 2.5 jar包和数据集上传到集群



```shell
#将测试数据拷贝到容器内
docker cp  ./HTTP_20130313143750.dat 6136bf0979d4:/root

#将jar包拷贝到容器内
docker cp ./MapReducerJob-1.0-SNAPSHOT-jar-with-dependencies.jar  6136bf0979d4:/root
```

数据机拷贝到HDFS集群路径下



```shell
#HDFS创建一个目录
/usr/local/hadoop-2.7.0/bin/hdfs dfs -mkdir /user/root/input01
# 查看目录
/usr/local/hadoop-2.7.0/bin/hdfs dfs -ls /

#本地文件上传到集群上
/usr/local/hadoop-2.7.0/bin/hdfs dfs -put HTTP_20130313143750.dat /user/root/input01
```

![img](https://cdn.nlark.com/yuque/0/2022/png/2981563/1647531488652-31413755-da0c-4969-8777-9cd89fa74887.png)





运行程序，版本不匹配down 。。。。   实验结束

,![img](https://cdn.nlark.com/yuque/0/2022/png/2981563/1647579930592-8ca82aed-0b08-46f5-83ab-889cd0f5424f.png)







## 3.运行环境更改到 阿里云hadoop集群 

上传jar，上传测试数据

```shell
scp MapReducerJob-1.0-SNAPSHOT-jar-with-dependencies.jar student3@114.55.52.33:/home/student3/afa

scp HTTP_20130313143750.dat student3@114.55.52.33:/home/student3/afa
```



将数据集上传到HDFS 集群

```shell
hdfs dfs -put HTTP_20130313143750.dat /home/student3/afa/input
```

运行jar



```shell
hadoop  jar MapReducerJob-1.0-SNAPSHOT-jar-with-dependencies.jar TrafficStatisticsDriver /home/student3/afa/input /home/student3/afa/output5
```



查看输出结果

![img](https://cdn.nlark.com/yuque/0/2022/png/2981563/1647592754357-601f20b9-c1a8-431d-9730-3bd9f8cc0604.png)
