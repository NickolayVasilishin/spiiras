package ru.nw.spiiras.nv.map;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.jnetpcap.Pcap;
import org.jnetpcap.packet.PcapPacket;
import org.jnetpcap.packet.PcapPacketHandler;
import org.jnetpcap.packet.format.FormatUtils;
import org.jnetpcap.protocol.network.Ip4;
import ru.nw.spiiras.nv.TrafficAnalyzerJob;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.logging.Logger;

/**
 * Created by Nick on 29.02.2016.
 */
public class IpHeaderMapper extends Mapper<NullWritable, NullWritable, Text, LongWritable> {
    private Pcap pcapFile;
    private boolean needSource = true;
    private Map<String, Long> ipEntries;
    private Ip4 header;
    private long processedPackets;

    static {
        System.loadLibrary(TrafficAnalyzerJob.LIBPCAP_LIBRARY);
        System.loadLibrary(TrafficAnalyzerJob.LIBPCAP_LIBRARY100);
    }


    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        //TODO check paths
        String fileName = ((FileSplit) context.getInputSplit()).getPath().toUri().getPath();
        Logger.getLogger(this.getClass().getSimpleName()).info("Got file to process: " + fileName);
        StringBuilder errors = new StringBuilder();
        pcapFile = Pcap.openOffline(fileName, errors);

        ipEntries = new HashMap<>();
        //get property - direction
        // init pcap
        //then read
    }

    @Override
    protected void map(NullWritable key, NullWritable value, Context context) throws IOException, InterruptedException {
        //TODO optimize
        pcapFile.loop(-1,
                new PcapPacketHandler<String>() {
            @Override
            public void nextPacket(PcapPacket packet, String user) {
                processedPackets++;
                header = new Ip4();
                if (!packet.hasHeader(header))
                    return;
                packet.getHeader(header);
                String ip = FormatUtils.ip(needSource ? header.source() : header.destination());
                if (ipEntries.containsKey(ip)) {
                    ipEntries.put(ip, ipEntries.get(ip) + 1);
                } else {
                    ipEntries.put(ip, 1L);
                }
            }
        },
                "nv");

        Logger.getLogger(this.getClass().getSimpleName()).info("Total packets processed: " + processedPackets);
        for(String k:ipEntries.keySet()) {
            context.write(new Text(k), new LongWritable(ipEntries.get(k)));
        }
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        pcapFile.close();
        super.cleanup(context);
    }
}
