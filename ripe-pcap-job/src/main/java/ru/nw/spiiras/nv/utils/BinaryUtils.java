package ru.nw.spiiras.nv.utils;

import java.nio.ByteOrder;

public class BinaryUtils {
    public BinaryUtils() {
    }

    public static byte[] flip(byte[] bytes, int length) {
        byte[] tmp = new byte[length];

        for(int i = 0; i < length; ++i) {
            tmp[i] = bytes[length - 1 - i];
        }

        return tmp;
    }

    public static byte[] flipBO(byte[] bytes, int length) {
        if(ByteOrder.nativeOrder().toString().equals("BIG_ENDIAN")) {
            return bytes;
        } else {
            byte[] tmp = new byte[length];

            for(int i = 0; i < length; ++i) {
                tmp[i] = bytes[length - 1 - i];
            }

            return tmp;
        }
    }

    public static byte[] flipBO(byte[] bytes) {
        return flipBO(bytes, bytes.length);
    }

    public static int byteToInt(byte[] b) {
        int dataLen = b.length;
        int idx = dataLen;
        int val = 0;
        int sum = 0;
        int bit_pos = 0;
        boolean out = false;

        while(true) {
            --idx;
            if(idx < 0) {
                return val;
            }

            while(bit_pos < 8) {
                int var7 = b[idx] >> bit_pos & 1;
                sum |= var7 << bit_pos;
                ++bit_pos;
            }

            val |= sum << (dataLen - 1 - idx) * 8;
            bit_pos = 0;
            sum = 0;
        }
    }

    public static int byteToInt(byte[] b, int len) {
        int dataLen = len;
        int idx = len;
        int val = 0;
        int sum = 0;
        int bit_pos = 0;
        boolean out = false;

        while(true) {
            --idx;
            if(idx < 0) {
                return val;
            }

            while(bit_pos < 8) {
                int var8 = b[idx] >> bit_pos & 1;
                sum |= var8 << bit_pos;
                ++bit_pos;
            }

            val |= sum << (dataLen - 1 - idx) * 8;
            bit_pos = 0;
            sum = 0;
        }
    }

    public static long ubyteToLong(byte[] b) {
        int i = 0;

        long val;
        for(val = 0L; i < b.length; ++i) {
            val |= ((long)b[i] & 255L) << (b.length - i - 1) * 8;
        }

        return val;
    }

    public static long ubyteToLong(byte[] b, int len) {
        byte[] newb = new byte[len];
        System.arraycopy(b, 0, newb, 0, newb.length);
        return ubyteToLong(newb);
    }

    public static long byteToLong(byte[] b) {
        if(b.length < 8) {
            return byteToLong(b, b.length);
        } else {
            int i = 0;
            long f = -72057594037927936L;

            long val;
            for(val = 0L; i < b.length; ++i) {
                val |= (long)(b[i] << (b.length - i) * 8) & f >> i * 8;
            }

            return val;
        }
    }

    public static long byteToLong(byte[] b, int len) {
        int dataLen = len;
        int idx = len;
        int sum = 0;
        int bit_pos = 0;
        boolean out = false;
        long val = 0L;

        while(true) {
            --idx;
            if(idx < 0) {
                return val;
            }

            while(bit_pos < 8) {
                int var9 = b[idx] >> bit_pos & 1;
                sum |= var9 << bit_pos;
                ++bit_pos;
            }

            val |= (long)(sum << (dataLen - 1 - idx) * 8);
            bit_pos = 0;
            sum = 0;
        }
    }

    public static byte[] LongToBytes(long lval) {
        byte[] bytes = new byte[8];
        int i = 0;

        for(long f = 255L; i < bytes.length; ++i) {
            bytes[i] = (byte)((int)(lval >> (bytes.length - 1 - i) * 8 & f));
        }

        return bytes;
    }

    public static byte[] IntToBytes(int val) {
        byte[] bytes = new byte[4];

        for(int i = 0; i < bytes.length; ++i) {
            bytes[i] = (byte)(val >> (bytes.length - 1 - i) * 8 & 255);
        }

        return bytes;
    }

    public static byte[] uIntToBytes(long val) {
        byte[] bytes = new byte[4];

        for(int i = 0; i < bytes.length; ++i) {
            bytes[i] = (byte)((int)(val >> (bytes.length - 1 - i) * 8 & 255L));
        }

        return bytes;
    }
}

