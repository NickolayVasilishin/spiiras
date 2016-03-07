package ru.nw.spiiras.nv.map;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.jnetpcap.JBufferHandler;
import org.jnetpcap.Pcap;
import org.jnetpcap.PcapHeader;
import org.jnetpcap.nio.JBuffer;
import org.jnetpcap.nio.JMemory;
import org.jnetpcap.packet.PcapPacket;
import org.jnetpcap.packet.PcapPacketHandler;
import org.jnetpcap.protocol.network.Ip4;
import ru.nw.spiiras.nv.TrafficAnalyzerJob;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 * Created by Nick on 29.02.2016.
 */
public class IpHeaderMapper extends Mapper<NullWritable, NullWritable, Text, LongWritable> {
    private Pcap pcapFile;
    private boolean checkingDestination = true;
    private Map<String, Long> ipEntries;
    static {
        System.loadLibrary(TrafficAnalyzerJob.LIBPCAP_LIBRARY);
        System.loadLibrary(TrafficAnalyzerJob.LIBPCAP_LIBRARY100);
    }

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        String fileName = ((FileSplit) context.getInputSplit()).getPath().getName();
        pcapFile = Pcap.openOffline(fileName, new StringBuilder());
        ipEntries = new HashMap<>();
        //get property - direction
        // init pcap
        //then read


    }

    @Override
    protected void map(NullWritable key, NullWritable value, Context context) throws IOException, InterruptedException {
        pcapFile.loop(-1, new PcapPacketHandler<String>() {
            @Override
            public void nextPacket(PcapPacket packet, String user) {
                Ip4 header = new Ip4();
                packet.getHeader(header);
                if(checkingDestination) {
                    if(ipEntries.containsKey(header.source())) ipEntries.put(header.source().toString(), ipEntries.get(header.source())+1)
                    else ipEntries.put(header.source(), 1);
                }
            }
        }, "nv");

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        super.cleanup(context);
    }
}
