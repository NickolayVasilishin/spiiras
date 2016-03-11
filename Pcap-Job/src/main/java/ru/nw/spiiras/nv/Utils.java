package ru.nw.spiiras.nv;

/**
 * Created by Nick on 09.03.2016.
 */
public class Utils {
    public static String ipToString(byte[] ip) {
        StringBuilder s = new StringBuilder();
        for(byte octet:ip) {
            s.append(octet);
            s.append('.');
        }
        s.deleteCharAt(s.lastIndexOf("."));
        return s.toString();
    }
}
