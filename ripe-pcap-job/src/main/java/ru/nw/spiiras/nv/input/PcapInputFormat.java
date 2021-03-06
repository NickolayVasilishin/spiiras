package ru.nw.spiiras.nv.input;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

public class PcapInputFormat extends FileInputFormat<LongWritable, BytesWritable> {

    public PcapInputFormat() {
    }

    public RecordReader<LongWritable, BytesWritable> createRecordReader(InputSplit split, TaskAttemptContext context) {
        return new PcapRecordReader();
    }

    protected boolean isSplitable(JobContext context, Path file) {
        return new CompressionCodecFactory(context.getConfiguration()).getCodec(file) == null;
    }
}
