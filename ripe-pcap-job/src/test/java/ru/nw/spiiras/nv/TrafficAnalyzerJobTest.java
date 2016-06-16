package ru.nw.spiiras.nv;

import net.ripe.hadoop.pcap.PcapReader;
import net.ripe.hadoop.pcap.packet.Packet;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.ObjectWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import ru.nw.spiiras.nv.map.IpHeaderMapper;
import ru.nw.spiiras.nv.reduce.IpHeaderReducer;

import java.io.DataInputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.lang.reflect.Field;
import java.util.LinkedList;

import static org.apache.hadoop.yarn.webapp.hamlet.HamletSpec.InputType.file;

/**
 * Created by Nick on 09.03.2016.
 */
public class TrafficAnalyzerJobTest {
    IpHeaderMapper mapper;
    IpHeaderReducer reducer;

    MapDriver<LongWritable, ObjectWritable, Text, LongWritable> mapDriver;
    ReduceDriver<Text, LongWritable, Text, LongWritable> reduceDriver;
    MapReduceDriver<LongWritable, ObjectWritable, Text, LongWritable, Text, LongWritable> mapReduceDriver;

    @Before
    public void setUp() {
        org.apache.log4j.BasicConfigurator.configure();

        mapper = new IpHeaderMapper();
        mapDriver = MapDriver.newMapDriver(mapper);
        mapDriver.setMapInputPath(new org.apache.hadoop.fs.Path("./src/test/resources/lil.pcap"));
//        PcapReader reader = new PcapReader(new DataInputStream(new FileInputStream(file)));
        reducer = new IpHeaderReducer();
        reduceDriver = ReduceDriver.newReduceDriver(reducer);

        mapReduceDriver = MapReduceDriver.newMapReduceDriver(mapper, reducer);
        mapReduceDriver.withMapInputPath(new org.apache.hadoop.fs.Path("./src/test/resources/lil.pcap"));

        System.setProperty( "java.library.path", System.getenv("HADOOP_HOME") + "/bin" );

        try {
            Field fieldSysPath = ClassLoader.class.getDeclaredField("sys_paths");
            fieldSysPath.setAccessible( true );
            fieldSysPath.set( null, null );
        } catch (NoSuchFieldException | IllegalAccessException e) {
            e.printStackTrace();
        }

    }

    @Test
    @Ignore
    public void testMapper() throws IOException {
        mapDriver.runTest();
    }

    @Test
    public void testReducer() throws IOException {
        LinkedList<LongWritable> values = new LinkedList<>();
        values.add(new LongWritable(10));
        values.add(new LongWritable(11));
        values.add(new LongWritable(12));
        values.add(new LongWritable(15));
        values.add(new LongWritable(16));
        values.add(new LongWritable(2));

        reduceDriver.withInput(new Text("192.168.1.1"), values)
                .withOutput(new Text("192.168.1.1"), new LongWritable(10+11+12+15+16+2))
                .runTest();
    }

    @Test
    @Ignore
    public void testJob() throws IOException {
        System.out.println(mapReduceDriver.run());
    }




}