package ru.nw.spiiras.nv.pcap;

import java.io.Closeable;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.logging.Logger;

import org.jnetpcap.nio.JBuffer;
import org.jnetpcap.packet.PcapPacket;

public class PcapSliceWriter implements PcapTransformer {
	private static final String PCAP_EXTENSION = ".pcap";
	private static final String PCAP_DEFAULT_OUTPUT_DIRECTORY = "target\\";
	private ExecutorService pool;
	private String directory;
	
	public PcapSliceWriter() {
		this(PCAP_DEFAULT_OUTPUT_DIRECTORY);
	}
	
	public PcapSliceWriter(String dir) {
		int workers = Runtime.getRuntime().availableProcessors() * 2;
		pool = Executors.newFixedThreadPool(workers);
		directory = dir;
		Logger.getLogger(this.getClass().getName()).info("Instantiated with " + workers + " workers.\nOutput directory is " + directory);
	}

	@Override
	public void apply(byte[] header, List<PcapPacket> packets, long count) {
		pool.execute(new Task(header, packets, count));
	}
	
	public void setOutputDirectory(String dir) {
		directory = dir;
	}
	
	private class Task implements Runnable{
		byte[] header;
		List<PcapPacket> packets;
		long count;
		
		Task(byte[] header, List<PcapPacket> packets, long count) {
			this.header = header;
			this.packets = packets;
			this.count = count;
		}
		
		@Override
		public void run() {
			try (DataOutputStream output = new DataOutputStream(new FileOutputStream(directory + count + PCAP_EXTENSION))){
				output.write(header);
				//TODO try to reuse instead of reallocation
				for (PcapPacket packet : packets){
					byte [] header = new byte[packet.getCaptureHeader().size()];
					packet.getCaptureHeader().transferTo(header, 0);
					output.write(header);
					byte [] buffer = new byte[packet.size()];
					JBuffer jb = new JBuffer(buffer);
					output.write(jb.getByteArray(0, packet.transferTo(jb)));
				}
			} catch (FileNotFoundException e) {
				Logger.getLogger(this.getClass().getName()).warning(e.getMessage());
				e.printStackTrace();
			} catch (IOException e1) {
				// TODO Auto-generated catch block
				e1.printStackTrace();
			}
		}
	}

	public void shutdown()  {
		pool.shutdown();
	}
	
	
}
