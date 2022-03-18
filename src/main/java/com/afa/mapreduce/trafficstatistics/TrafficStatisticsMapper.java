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
