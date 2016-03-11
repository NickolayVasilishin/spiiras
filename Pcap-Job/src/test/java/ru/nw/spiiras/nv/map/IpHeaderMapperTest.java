package ru.nw.spiiras.nv.map;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.Before;
import org.junit.Test;
import ru.nw.spiiras.nv.reduce.IpHeaderReducer;

import java.io.IOException;
import java.lang.reflect.Field;

import static org.junit.Assert.*;

/**
 * Created by Nick on 09.03.2016.
 */
public class IpHeaderMapperTest {
    IpHeaderMapper mapper = new IpHeaderMapper();
    IpHeaderReducer reducer = new IpHeaderReducer();

    MapDriver<NullWritable, NullWritable, Text, LongWritable> mapDriver;
    ReduceDriver<Text, LongWritable, Text, LongWritable> reduceDriver;
    MapReduceDriver<NullWritable, NullWritable, Text, LongWritable, Text, LongWritable> mapReduceDriver;

    @Before
    public void setUp() {


        mapDriver = MapDriver.newMapDriver(mapper);
        mapDriver.setMapInputPath(new org.apache.hadoop.fs.Path("./src/test/resources/1.pcap"));
        reduceDriver = ReduceDriver.newReduceDriver(reducer);
        mapReduceDriver = MapReduceDriver.newMapReduceDriver(mapper, reducer);
//        mapDriver.addCacheFile("./src/main/resources/native/windows/x86_64/jnetpcap.dll");
//        mapDriver.addCacheFile("./src/main/resources/native/windows/x86_64/jnetpcap-pcap100.dll");
//        mapDriver.addCacheFile("./src/main/resources/native/windowss/");
//        mapDriver.addCacheFile("./src/main/resources/native/linux/amd64/libjnetpcap.so");
//        mapDriver.addCacheFile("./src/main/resources/native/linux/amd64/libjnetpcap-pcap100.so");

        System.setProperty( "java.library.path", System.getenv("HADOOP_HOME") + "/bin" );
        try {
            Field fieldSysPath = ClassLoader.class.getDeclaredField( "sys_paths" );
            fieldSysPath.setAccessible( true );
            fieldSysPath.set( null, null );
        } catch (NoSuchFieldException | IllegalAccessException e) {
            e.printStackTrace();
        }

    }

    @Test
    public void testMapper() throws IOException {
        mapDriver.withInput(NullWritable.get(), NullWritable.get()).runTest();
    }


}