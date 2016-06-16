package ru.nw.spiiras.nv.utils;

public class Bytes {
    public Bytes() {
    }

    public static int toInt(byte[] src, int srcPos) {
        int dword = 0;

        for(int i = 0; i < src.length - srcPos; ++i) {
            dword = (dword << 8) + (src[i + srcPos] & 127);
            if((src[i + srcPos] & 128) == 128) {
                dword += 128;
            }
        }

        return dword;
    }

    public static int toInt(byte[] src) {
        return toInt(src, 0);
    }

    public static int toInt(byte src) {
        byte[] b = new byte[]{src};
        return toInt(b, 0);
    }

    public static long toLong(byte[] src, int srcPos) {
        long dword = 0L;

        for(int i = 0; i < src.length - srcPos; ++i) {
            dword = (dword << 8) + (long)(src[i + srcPos] & 127);
            if((src[i + srcPos] & 128) == 128) {
                dword += 128L;
            }
        }

        return dword;
    }

    public static long toLong(byte[] src) {
        return toLong(src, 0);
    }
}
