package ru.nw.spiiras.nv.input;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.*;

import java.io.IOException;
import java.util.List;

/**
 * Created by Nick on 29.02.2016.
 */
public class PcapInputFormat extends InputFormat<Text, BytesWritable> {
    @Override
    public List<InputSplit> getSplits(JobContext jobContext) throws IOException, InterruptedException {

        return null;
    }

    @Override
    public RecordReader<Text, BytesWritable> createRecordReader(InputSplit inputSplit, TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
        return null;
    }
}
