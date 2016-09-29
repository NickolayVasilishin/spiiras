package ru.nw.spiiras.nv;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import ru.nw.spiiras.nv.input.PcapInputFormat;
import ru.nw.spiiras.nv.lib.PacketStatisticsWritable;
import ru.nw.spiiras.nv.map.PacketStatisticsMapper;
import ru.nw.spiiras.nv.reduce.PacketStatisticsReducer;

import java.io.IOException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Created by Nick on 17.06.2016.
 */
public class ApplicationManager {
    private MyUI applicationUI;
    private ExecutorService jobExecutor;
    private JobExecutor daemon;

    public ApplicationManager(MyUI ui) {
        jobExecutor = Executors.newSingleThreadExecutor();
        applicationUI = ui;
    }

    public void start() {
        daemon = new JobExecutor();
        jobExecutor.execute(daemon);
    }

    public void shutdown() {
        daemon.stop();
        jobExecutor.shutdown();
    }

    private class JobExecutor implements Runnable {
        private volatile boolean stop;

        public void stop() {
            stop = true;
        }

        @Override
        public void run() {
                Configuration conf = new Configuration();
                conf.set("yarn.resourcemanager.address", "localhost:8050");
                conf.set("mapreduce.framework.name", "yarn");
                conf.set("fs.defaultFS", "hdfs://<your-hostname>/");
                conf.set("yarn.application.classpath",
                        "$HADOOP_CONF_DIR,$HADOOP_COMMON_HOME/*,$HADOOP_COMMON_HOME/lib/*,"
                                + "$HADOOP_HDFS_HOME/*,$HADOOP_HDFS_HOME/lib/*,"
                                + "$HADOOP_YARN_HOME/*,$HADOOP_YARN_HOME/lib/*,"
                                + "$HADOOP_MAPRED_HOME/*,$HADOOP_MAPRED_HOME/lib/*");
                Job job = null;
                try {
                    job = Job.getInstance(conf, "Traffic Analysis");
                } catch (IOException e) {
                    e.printStackTrace();
                }

                job.setJar("\\tmp\\jars\\trafficjob.jar");
                job.setJobName("Traffic Analysis");
                job.setOutputKeyClass(LongWritable.class);
                job.setOutputValueClass(Text.class);
                job.setMapperClass(PacketStatisticsMapper.class);
                job.setNumReduceTasks(5);
                job.setReducerClass(PacketStatisticsReducer.class);
                job.setMapOutputKeyClass(LongWritable.class);
                job.setMapOutputValueClass(PacketStatisticsWritable.class);
                job.setInputFormatClass(PcapInputFormat.class);
                job.setOutputFormatClass(TextOutputFormat.class);

                // Job Input path
                try {
                    FileInputFormat.addInputPath(job, new
                            Path("hdfs://localhost:54310/user/input/"));
                } catch (IOException e) {
                    e.printStackTrace();
                }
                // Job Output path
                FileOutputFormat.setOutputPath(job, new
                        Path("hdfs://localhost:54310/user/output"));
            while (!stop) {
                try {
                    job.waitForCompletion(false);
                    Thread.sleep(5*60*1000);
                } catch (IOException e) {
                    e.printStackTrace();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                } catch (ClassNotFoundException e) {
                    e.printStackTrace();
                }
            }
        }
    }
}
