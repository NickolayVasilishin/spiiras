package ru.nw.spiiras.nv.map;

import net.ripe.hadoop.pcap.packet.DnsPacket;
import net.ripe.hadoop.pcap.packet.Packet;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Mapper;
import ru.nw.spiiras.nv.input.PartialPcapReader;
import ru.nw.spiiras.nv.lib.PacketStatisticsWritable;

import java.io.IOException;

/**
 * Created by Nick on 16.06.2016.
 */
public class PacketStatisticsMapper extends Mapper<LongWritable, BytesWritable, LongWritable, PacketStatisticsWritable> {

    private final static int AVERAGE_MAXIMUM_URL_LENGTH = 217;
    private final static int AVERAGE_MINIMUM_URL_LENGTH = 37;

    public enum Fields {
        IP,
        SYN,
        ACK,
        DNS_HUGE,
        DNS_SMALL,
        DNS,
        HTTP
    }
    private long period;

    public PacketStatisticsMapper(long period) {
        this.period = period;
    }

    @Override
    protected void map(LongWritable key, BytesWritable value, Context context) throws IOException, InterruptedException {
        Packet packet = new PartialPcapReader(value.getBytes()).iterator().next();
        PacketStatisticsWritable statistics = new PacketStatisticsWritable(Fields.values().length);
        key = new LongWritable(roundToPeriod(packet.get(Packet.TIMESTAMP)));
        statistics.incrementField(Fields.IP);
        if (packet.get(Packet.PROTOCOL).equals("TCP")) {
            if ((boolean) packet.get(Packet.TCP_FLAG_ACK)) {
                statistics.incrementField(Fields.ACK);
            } else if ((boolean) packet.get(Packet.TCP_FLAG_SYN)) {
                statistics.incrementField(Fields.SYN);
            }
        }
        if (packet.get(Packet.PROTOCOL).equals("DNS")) {
            statistics.incrementField(Fields.DNS);
            if (((String) packet.get(DnsPacket.QUESTION)).length() > AVERAGE_MAXIMUM_URL_LENGTH) {
                statistics.incrementField(Fields.DNS_HUGE);
            } else {
                if (((String) packet.get(DnsPacket.QUESTION)).length() < AVERAGE_MINIMUM_URL_LENGTH) {
                    statistics.incrementField(Fields.DNS_SMALL);
                }
            }
            if (packet.get(Packet.PROTOCOL).equals("HTTP")) {
                statistics.incrementField(Fields.HTTP);
            }
            context.write(key, statistics);
        }
    }

    private long roundToPeriod(Object timestamp) {
        return roundToPeriod((long) timestamp);
    }

    private long roundToPeriod(long timestamp) {
        return timestamp - timestamp % period;
    }
}
