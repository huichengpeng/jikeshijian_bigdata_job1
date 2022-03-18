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
