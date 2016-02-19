package ru.nw.spiiras.nv.pcap;

import java.io.IOException;
import java.util.List;

import org.jnetpcap.PcapClosedException;
import org.jnetpcap.packet.PcapPacket;

public class PcapConverter {

	public static void main(String[] args) {
		String captureFile = "resources\\capturefile_small.pcap";
		
		if(args.length > 1) {
			captureFile = args[1];
		}
		
		PcapTransformer t = new PcapSliceWriter();	
		try (PcapReader r = new PcapReader(captureFile);){
			System.out.println("Starting...");
			while(!r.isEmpty()){
				List<PcapPacket> packets = r.slice();
				t.apply(r.getGlobalHeader(), packets, r.getSliceCount());
			}
		} catch (PcapClosedException e) {
			System.err.println(e.getMessage());
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			((PcapSliceWriter) t).shutdown();
		}
		System.out.println("Finished.");
	}

}
