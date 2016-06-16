package ru.nw.spiiras.nv.lib;

import org.apache.hadoop.io.Writable;
import ru.nw.spiiras.nv.map.PacketStatisticsMapper;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;

/**
 * Created by Nick on 16.06.2016.
 */
public class PacketStatisticsWritable implements Writable {
    private long[] fields;

    public PacketStatisticsWritable(int numberOfFields) {
        fields = new long[numberOfFields];
    }

    public void setField(int index, long value) {
        fields[index] = value;
    }

    public long getField(int index) {
        return fields[index];
    }

    public <T extends PacketStatisticsMapper.Fields> void incrementField(T field) {
        incrementField(field.ordinal());
    }

    public void incrementField(int index) {
        addToField(index, 1);
    }

    public void addToField(int index, long value) {
        fields[index] = fields[index] + value;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        for (long value:fields) {
            dataOutput.writeLong(value);
        }
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        byte[] data = new byte[0];
        dataInput.readFully(data);
        ByteBuffer.wrap(data).asLongBuffer().get(fields);
    }

    public PacketStatisticsWritable merge(PacketStatisticsWritable other) {
        PacketStatisticsWritable newStatistics = new PacketStatisticsWritable(this.fields.length);
        for(int i = 0; i < fields.length; i++) {
            newStatistics.fields[i] = this.fields[i] + other.fields[i];
        }
        return newStatistics;
    }

    @Override
    public String toString() {
        return Arrays.toString(fields).replace("[", "").replace("]","");
    }
}
