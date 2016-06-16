package ru.nw.spiiras.nv.reduce;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.logging.Logger;

/**
 * Created by Nick on 29.02.2016.
 */
public class IpHeaderReducer extends Reducer<Text, LongWritable, Text, LongWritable> {
    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        Logger.getLogger(this.getClass().getSimpleName()).info("Reducer at setup()");

        super.setup(context);
    }

    @Override
    protected void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
        Long count = 0L;
        for(LongWritable value:values) {
            count += value.get();
        }
        context.write(key, new LongWritable(count));
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        super.cleanup(context);
    }
}
