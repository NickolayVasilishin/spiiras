package ru.nw.spiiras.nv.map;

import net.ripe.hadoop.pcap.packet.Packet;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.ObjectWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

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

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        //TODO check paths
        String fileName = ((FileSplit) context.getInputSplit()).getPath().toUri().getPath();
        Logger.getLogger(this.getClass().getSimpleName()).info("Got file to process: " + fileName);
        ipEntries = new HashMap<>();
        //get property - direction
    }

    @Override
    protected void map(LongWritable key, ObjectWritable value, Context context) throws IOException, InterruptedException {
        Packet packet = (Packet) value.get();
        if(packet == null || packet.get(Packet.SRC) == null)
            return;
        String address = packet.get(Packet.SRC).toString();
        put(address);
        //Отправляем результат работы map-функции
        write(context);
    }

    private void write(Context context) throws IOException, InterruptedException {
        for(String k:ipEntries.keySet()) {
            if(k.trim().equals(""))
                continue;
            context.write(new Text(k), new LongWritable(ipEntries.get(k)));
        }
    }

    private void put(String address) {
        if(ipEntries.containsKey(address))
            ipEntries.put(address, ipEntries.get(address)+1);
        else
            ipEntries.put(address, 1L);
    }
}
