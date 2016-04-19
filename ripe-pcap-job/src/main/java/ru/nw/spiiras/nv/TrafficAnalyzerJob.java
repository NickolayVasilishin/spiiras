package ru.nw.spiiras.nv;

import net.ripe.hadoop.pcap.io.CombinePcapInputFormat;
import net.ripe.hadoop.pcap.io.PcapInputFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import ru.nw.spiiras.nv.map.IpHeaderMapper;
import ru.nw.spiiras.nv.reduce.IpHeaderReducer;

import java.net.URI;

/**
 * Created by Nick on 29.02.2016.
 */
public class TrafficAnalyzerJob extends Configured implements Tool {

    public static void main(String[] args) throws Exception {
        if(args.length < 1)
            System.out.println("Usage: input-files ... output-file");
        int res = ToolRunner.run(new Configuration(), new TrafficAnalyzerJob(), args);
        System.exit(res);
    }

    @Override
    public int run(String[] args) throws Exception {
        // When implementing tool
        Configuration conf = this.getConf();

        // Create job
        Job job = new Job(conf, "Ip Counter Job");
        job.setJarByClass(TrafficAnalyzerJob.class);

        job.setInputFormatClass(PcapInputFormat.class);
        // Setup MapReduce job
        // Do not specify the number of Reducer
        job.setMapperClass(IpHeaderMapper.class);
        job.setReducerClass(IpHeaderReducer.class);
        job.setNumReduceTasks(1);

        // Specify key / value
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);

        // Input
//        for(int i = 0; i < args.length - 1; i++)
//        FileInputFormat.setInputDirRecursive(job, true);
            FileInputFormat.addInputPath(job, new Path(args[0]));

        // Output
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        job.setOutputFormatClass(TextOutputFormat.class);

        // Execute job and return status
        return job.waitForCompletion(true) ? 0 : 1;
    }

}
