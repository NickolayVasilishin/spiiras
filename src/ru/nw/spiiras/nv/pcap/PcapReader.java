package ru.nw.spiiras.nv.pcap;

import java.io.Closeable;
import java.io.DataInputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.LinkedList;
import java.util.List;
import java.util.logging.Logger;

import org.jnetpcap.Pcap;
import org.jnetpcap.PcapClosedException;
import org.jnetpcap.nio.JMemory.Type;
import org.jnetpcap.packet.PcapPacket;
import org.jnetpcap.packet.PcapPacketHandler;

public class PcapReader implements Closeable {

	// overhead 0.00036

	private static final long PCAP_MAXIMUM_SLICE_SIZE = 64 * 1024 * 1024;
	private static final long PCAP_PACKET_TRESHOLD = 2000;
	private static final int PCAP_GLOBAL_HEADER_LENGTH = 24;
	private final byte[] globalHeader;
	private StringBuilder errbuf;
	private Pcap pcap;
	private long sliceCount = 0;
	private List<PcapPacket> packets;
	private PacketHandler handler;
	private boolean empty = false;

	public PcapReader(String pcapFile) throws IOException, PcapClosedException {
		// Getting global header
		DataInputStream byteStream = new DataInputStream(new FileInputStream(
				pcapFile));
		globalHeader = new byte[PCAP_GLOBAL_HEADER_LENGTH];
		byteStream.read(globalHeader);
		byteStream.close();

		errbuf = new StringBuilder();
		pcap = Pcap.openOffline(pcapFile, errbuf);
		if (pcap == null)
			throw new PcapClosedException(
					"Error while opening device for capture: "
							+ errbuf.toString());

		packets = new LinkedList<>();
		handler = new PacketHandler();
	}

	@Override
	public void close() {
		pcap.close();
	}

	public boolean isEmpty() {
		return empty;
	}

	public byte[] getGlobalHeader() {
		return globalHeader;
	}

	public String getError() {
		return errbuf.toString();
	}

	public long getSliceCount() {
		return sliceCount;
	}

	public List<PcapPacket> slice() {
		// TODO handle other codes
		if (pcap.loop(-1, handler, "nv") == 0) {
			empty = true;
			Logger.getLogger("PcapReader").info(
					"Processed " + handler.getCount()
							+ " packets.\nNo more packets to process.");
		}
		List<PcapPacket> p = packets;
		sliceCount++;
		handler.clean();
		return p;
	}

	private class PacketHandler implements PcapPacketHandler<String> {
		private long count = 0;
		private long totalSize = 0;

		@Override
		public void nextPacket(PcapPacket packet, String user) {
			// TODO check if this size is correct
			totalSize += packet.getTotalSize();
			count++;
			// TODO CHECK
			packets.add(packet);
			if (totalSize >= PCAP_MAXIMUM_SLICE_SIZE - PCAP_PACKET_TRESHOLD) {
				Logger.getLogger("PcapReader").info(
						"Recorded " + count + " packets.");
				pcap.breakloop();
			}
		}

		public long getCount() {
			return count;
		}

		private void clean() {
			count = 0;
			totalSize = 0;
			packets = new LinkedList<>();
		}
	}

}
