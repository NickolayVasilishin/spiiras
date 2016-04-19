package ru.nw.spiiras.nv.map;

import net.ripe.hadoop.pcap.packet.Packet;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import ru.nw.spiiras.nv.TrafficAnalyzerJob;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.logging.Logger;

/**
 * Created by Nick on 29.02.2016.
 */
public class IpHeaderMapper extends Mapper<LongWritable, ObjectWritable, Text, LongWritable> {
    private boolean needSource = true;
    private Map<String, Long> ipEntries;
    private long processedPackets;
    private Packet packet;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        //TODO check paths
        String fileName = ((FileSplit) context.getInputSplit()).getPath().toUri().getPath();
        Logger.getLogger(this.getClass().getSimpleName()).info("Got file to process: " + fileName);
        StringBuilder errors = new StringBuilder();

        ipEntries = new HashMap<>();
        //get property - direction
        // init pcap
        //then read
    }

    @Override
    protected void map(LongWritable key, ObjectWritable value, Context context) throws IOException, InterruptedException {
        //TODO optimize

        String address = "";
        packet = (Packet) value.get();
        if(packet != null)
            address = packet.get(Packet.SRC).toString();
        if(ipEntries.containsKey(address))
            ipEntries.put(address, ipEntries.get(address)+1);
        else
            ipEntries.put(address, 1L);

        Logger.getLogger(this.getClass().getSimpleName()).info("Total packets processed: " + processedPackets);

        for(String k:ipEntries.keySet()) {
            if(k.trim().equals(""))
                continue;
            context.write(new Text(k), new LongWritable(ipEntries.get(k)));
        }
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        super.cleanup(context);
    }
}
