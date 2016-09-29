package ru.nw.spiiras.nv.reduce;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import ru.nw.spiiras.nv.lib.PacketStatisticsWritable;
import ru.nw.spiiras.nv.map.PacketStatisticsMapper;

import java.io.IOException;

/**
 * Created by Nick on 16.06.2016.
 */
public class PacketStatisticsReducer extends Reducer<LongWritable, PacketStatisticsWritable, LongWritable, Text> {

    private static final int STATS_FIELDS_NUMBER = PacketStatisticsMapper.Fields.values().length;


    @Override
    protected void reduce(LongWritable key, Iterable<PacketStatisticsWritable> values, Context context) throws IOException, InterruptedException {
        PacketStatisticsWritable consumer = new PacketStatisticsWritable(STATS_FIELDS_NUMBER);
        for(PacketStatisticsWritable stat:values) {
            consumer = consumer.merge(stat);
        }
        context.write(key, new Text(consumer.toString()));
    }
}
