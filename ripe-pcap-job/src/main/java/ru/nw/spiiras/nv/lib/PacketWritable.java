package ru.nw.spiiras.nv.lib;

import net.ripe.hadoop.pcap.packet.Packet;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import ru.nw.spiiras.nv.input.PartialPcapReader;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Created by Nick on 16.06.2016.
 */
public class PacketWritable implements WritableComparable<PacketWritable> {
    public Packet fields;
    private BytesWritable packetBinary;
    private BooleanWritable toServer;

    public PacketWritable(BytesWritable packetBinary) throws IOException {
        this.packetBinary = packetBinary;
        fields = new PartialPcapReader(packetBinary.getBytes()).iterator().next();
        toServer = new BooleanWritable(false);
    }


    public void toServer() {
        toServer.set(true);
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        packetBinary.write(dataOutput);
        toServer.write(dataOutput);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        packetBinary.readFields(dataInput);
        toServer.readFields(dataInput);
        fields = new PartialPcapReader(packetBinary.getBytes()).iterator().next();
    }

    @Override
    public int compareTo(PacketWritable o) {
        return (int) Math.signum((long) fields.get(Packet.TCP_SEQ) - (long) o.fields.get(Packet.TCP_SEQ));
    }
}
