package ru.nw.spiiras.nv.pcap;

import java.io.Closeable;
import java.io.DataInputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.List;

/**
 * Created by Nick on 24.02.2016.
 */
public class PcapByteReader implements Closeable, PcapReader {
    private static int PCAP_GLOBAL_HEADER_LENGTH = 24;
    private DataInputStream byteStream;
    private byte[] globalHeader;

    public PcapByteReader(String pcapFile) throws IOException {
        byteStream = new DataInputStream(new FileInputStream(pcapFile));
        globalHeader = new byte[PCAP_GLOBAL_HEADER_LENGTH];
        if (byteStream.read(globalHeader) != PCAP_GLOBAL_HEADER_LENGTH)
            throw new IOException("Wrong pcap file.");
        if (!validateGlobalHeader())
            throw new IOException("Wrong pcap file.");
    }

    //TODO Implement
    private boolean validateGlobalHeader() {
        return true;
    }

    @Override
    public void close() throws IOException {
        byteStream.close();
    }

    @Override
    public List slice() {
        return null;
    }

    @Override
    public boolean hasNext() {
        return false;
    }
}
