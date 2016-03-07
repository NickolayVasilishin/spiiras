package ru.nw.spiiras.nv.reduce;

import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Created by Nick on 29.02.2016.
 */
public class IpHeaderReducer extends Reducer {
    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        super.setup(context);
    }

    @Override
    protected void reduce(Object key, Iterable values, Context context) throws IOException, InterruptedException {
        super.reduce(key, values, context);
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        super.cleanup(context);
    }
}
