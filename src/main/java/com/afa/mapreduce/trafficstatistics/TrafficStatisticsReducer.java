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
