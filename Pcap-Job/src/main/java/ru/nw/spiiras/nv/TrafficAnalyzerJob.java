package ru.nw.spiiras.nv;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;

/**
 * Created by Nick on 29.02.2016.
 */
public class TrafficAnalyzerJob extends Configured implements Tool {
    public static final String LIBPCAP_LIBRARY = "libjnetpcap.so";
    public static final String LIBPCAP_LIBRARY100 = "libjnetpcap-pcap100.so";

    @Override
    public int run(String[] strings) throws Exception {
        return 0;
    }

}
