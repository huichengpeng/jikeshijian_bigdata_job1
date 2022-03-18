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
