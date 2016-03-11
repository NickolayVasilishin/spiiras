package ru.nw.spiiras.nv.pcap;

import java.util.List;

/**
 * Created by Nick on 24.02.2016.
 */
public interface PcapReader<PcapItem> {
    public List<PcapItem> slice();
    public boolean hasNext();
}
