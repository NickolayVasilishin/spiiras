package ru.nw.spiiras.nv.map;

import net.ripe.hadoop.pcap.packet.Packet;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.join.TupleWritable;
import ru.nw.spiiras.nv.lib.PacketWritable;

import java.io.IOException;

/**
 * Created by Nick on 16.06.2016.
 */
public class TcpFlowMapper extends Mapper<LongWritable, BytesWritable, TupleWritable, PacketWritable> {
    @Override
    protected void map(LongWritable key, BytesWritable value, Context context) throws IOException, InterruptedException {
        PacketWritable packet = new PacketWritable(value);
        if (packet.fields.get(Packet.PROTOCOL).equals("TCP") || packet.fields.get(Packet.PROTOCOL).equals("UDP")) {
            context.write(getTuple(packet), packet);
        }
    }

    private TupleWritable getTuple(PacketWritable packet) {
        String sourceIp = (String) packet.fields.get(Packet.SRC);
        Integer sourcePort = (Integer) packet.fields.get(Packet.SRC_PORT);
        String destinationIp = (String) packet.fields.get(Packet.DST);
        Integer destinationPort = (Integer) packet.fields.get(Packet.DST_PORT);
        String protocol = (String) packet.fields.get(Packet.PROTOCOL);

        Writable[] tuple = new Writable[5];

        if(sourcePort > destinationPort) {
            tuple[0] = new Text(sourceIp);
            tuple[1] = new IntWritable(sourcePort);
            tuple[2] = new Text(destinationIp);
            tuple[3] = new IntWritable(destinationPort);
            packet.toServer();
        } else {
            tuple[0] = new Text(destinationIp);
            tuple[1] = new IntWritable(destinationPort);
            tuple[2] = new Text(sourceIp);
            tuple[3] = new IntWritable(sourcePort);
        }
        tuple[4] = new Text(protocol);

        return new TupleWritable(tuple);
    }
}
