package ru.nw.spiiras.nv.input;

import net.ripe.hadoop.pcap.PcapReader;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.io.SequenceInputStream;

/**
 * Created by Nick on 16.06.2016.
 */
public class PartialPcapReader extends PcapReader {
    public static final byte[] PCAP_DEFAULT_GLOBAL_HEADER = new byte[]
            {
                    -44, -61, -78, -95,
                    2, 0,
                    4, 0,
                    0, 0, 0, 0,
                    0, 0, 0, 0,
                    -1, -1, 0, 0,
                    1, 0, 0, 0
            };

    public PartialPcapReader(DataInputStream is) throws IOException {
        super(new DataInputStream(new SequenceInputStream(new ByteArrayInputStream(PCAP_DEFAULT_GLOBAL_HEADER), is)));
    }

    public PartialPcapReader(byte[] array) throws IOException {
        this(new DataInputStream(new ByteArrayInputStream(array)));
    }
}
