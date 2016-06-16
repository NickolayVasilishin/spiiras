package ru.nw.spiiras.nv.input;

import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.io.SequenceInputStream;

import static ru.nw.spiiras.nv.input.PartialPcapReader.PCAP_DEFAULT_GLOBAL_HEADER;

/**
 * Created by Nick on 16.06.2016.
 */
public class PartialPcapReaderTest {

    @Test
    public void test() throws IOException {
        DataInputStream is = new DataInputStream(new ByteArrayInputStream(new byte[]{127, 127, 127, 0x7f}));
        DataInputStream s = new DataInputStream(new SequenceInputStream(new ByteArrayInputStream(PCAP_DEFAULT_GLOBAL_HEADER), is));
//        DataInputStream s1 = new DataInputStream(new ByteArrayInputStream(PCAP_DEFAULT_GLOBAL_HEADER));
//        byte[] b = new byte[24];
//        s.readFully(b);
//        System.out.println(b);
//        System.out.println(s.readByte());
//        System.out.println(s.readByte());
//        System.out.println(s.readByte());
//        System.out.println(s.readByte());
//        System.out.println(s.readByte());
//        System.out.println(s.readByte());
//        System.out.println(s.readByte());
//        System.out.println(s.readByte());
//        System.out.println(s.readByte());
//        System.out.println(s.readByte());
//        System.out.println(s.readByte());
//        System.out.println(s.readByte());
        new PartialPcapReader(is);
    }

}