package ru.nw.spiiras.nv.pcap;

import java.util.List;

import org.jnetpcap.packet.PcapPacket;

public interface PcapTransformer {
	public void apply(byte[] header, List<PcapPacket> packets, long count);
	public void shutdown();
}
