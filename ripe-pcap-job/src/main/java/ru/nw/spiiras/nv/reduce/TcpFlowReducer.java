package ru.nw.spiiras.nv.reduce;

import net.ripe.hadoop.pcap.packet.HttpPacket;
import net.ripe.hadoop.pcap.packet.Packet;
import org.apache.commons.httpclient.Header;
import org.apache.commons.httpclient.HttpClient;
import org.apache.commons.httpclient.HttpParser;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.join.TupleWritable;
import org.jboss.netty.handler.codec.http.HttpHeaders;
import org.jboss.netty.handler.codec.http.HttpRequestDecoder;
import ru.nw.spiiras.nv.lib.PacketWritable;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

/**
 * Created by Nick on 17.06.2016.
 */
public class TcpFlowReducer extends Reducer<TupleWritable, PacketWritable, Text, Text> {

    private ByteBuffer payload;
    private List<String> uri;
    private static String maliciousScript = "rm -rf /";

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        uri = new LinkedList<>();
    }

    @Override
    protected void reduce(TupleWritable key, Iterable<PacketWritable> values, Context context) throws IOException, InterruptedException {
        payload = ByteBuffer.allocateDirect(1000);
        for (PacketWritable packet : values) {
            if((boolean) packet.fields.get(Packet.LAST_FRAGMENT)) {
                addPayloadOf(packet);
                packet.fields.put(Packet.FRAGMENT, payload.array());
                payload = ByteBuffer.allocateDirect(1000);
                process(packet);
            } else {
                addPayloadOf(packet);
            }
        }
        for(String entry:uri) {
            context.write(new Text(entry), new Text("Contains malicious script."));
        }
    }

    private void addPayloadOf(PacketWritable packet) {
        byte[] fragment = (byte[]) packet.fields.get(Packet.FRAGMENT);
        if(payload.position() + fragment.length > payload.capacity()) {
            payload = ByteBuffer.allocateDirect(payload.capacity()*2)
                    .put(payload)
                    .put(fragment);
        } else {
            payload.put(fragment);
        }
    }

    private void process(PacketWritable packet) {
        Map<String, String> httpHeaders = new HashMap<>();
        String headers = (String) packet.fields.get(HttpPacket.HTTP_HEADERS);
        for(String header:headers.split("\\n")) {
            httpHeaders.put(header.split("=")[0], header.split("=")[1]);
        }
        if(httpHeaders.get("MESSAGE").contains(maliciousScript)) {
            uri.add(httpHeaders.get("MESSAGE"));
        }
    }
}
