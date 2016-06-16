package ru.nw.spiiras.nv.InputFormat;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.BytesWritable;
import ru.nw.spiiras.nv.utils.BinaryUtils;
import ru.nw.spiiras.nv.utils.Bytes;


import java.io.IOException;
import java.io.InputStream;

public class PcapLineReader {
    private static final int DEFAULT_BUFFER_SIZE = 2048;
    private int bufferSize;
    private static final int PCAP_FILE_HEADER_LENGTH = 24;
    private static final int PCAP_PACKET_HEADER_LENGTH = 16;
    private static final int PCAP_PACKET_HEADER_CAPLEN_POS = 8;
    private static final int PCAP_PACKET_HEADER_WIREDLEN_POS = 12;
    private static final int PCAP_PACKET_HEADER_CAPLEN_LEN = 4;
    private static final int PCAP_PACKET_HEADER_TIMESTAMP_LEN = 4;
    private static final int PCAP_PACKET_MIN_LEN = 53;
    private static final int PCAP_PACKET_MAX_LEN = 1519;
    private static final int MAGIC_NUMBER = -725372255;
    private static final int MIN_PKT_SIZE = 42;
    private static final long PCAP_TIMESTAMP_THRESHOLD = 100L;
    //На основании GlobalHeader
    private long minCaptureTime;
    private long maxCaptureTime;

    private InputStream in;
    private byte[] buffer;
    private byte[] pcapHeader;
    private int bufferLength;
    private int consumed;

    public PcapLineReader(InputStream in, int bufferSize, long minCaptureTime, long maxCaptureTime) {
        this.bufferSize = DEFAULT_BUFFER_SIZE;
        this.bufferLength = 0;
        this.consumed = 0;
        this.in = in;
        this.bufferSize = bufferSize;
        this.buffer = new byte[this.bufferSize];
        this.minCaptureTime = minCaptureTime;
        this.maxCaptureTime = maxCaptureTime;
    }

    public PcapLineReader(InputStream in, Configuration conf) throws IOException {
        this(in, DEFAULT_BUFFER_SIZE, conf.getLong("pcap.file.captime.min", 1309412600L), conf.getLong("pcap.file.captime.max", 1309585400L));
    }

    public void close() throws IOException {
        in.close();
    }

    int skipPartialRecord(int fraction) throws IOException {
        int pos = 0;
        byte[] captured = new byte[fraction];

        byte[] tmpTimestamp1 = new byte[4];
        byte[] tmpCapturedLen1 = new byte[4];
        byte[] tmpWiredLen1 = new byte[4];

        byte[] tmpTimestamp2 = new byte[4];
        byte[] tmpCapturedLen2 = new byte[4];
        byte[] tmpWiredLen2 = new byte[4];

        int caplen1;
        int wiredlen1;
        int caplen2;
        int wiredlen2;

        int size;
        long timestamp1;
        long timestamp2;
        if ((size = in.read(captured)) < MIN_PKT_SIZE) {
            return 0;
        } else {
            for (; pos < size; ++pos) {
                if (size - pos < 32 || size - pos < PCAP_PACKET_MIN_LEN) {
                    pos = size;
                    break;
                }

                System.arraycopy(captured, pos, tmpTimestamp1, 0, PCAP_PACKET_HEADER_TIMESTAMP_LEN);
                timestamp1 = Bytes.toLong(BinaryUtils.flipBO(tmpTimestamp1, PCAP_PACKET_HEADER_TIMESTAMP_LEN));
                System.arraycopy(captured, pos + PCAP_PACKET_HEADER_CAPLEN_POS, tmpCapturedLen1, 0, PCAP_PACKET_HEADER_CAPLEN_LEN);
                caplen1 = Bytes.toInt(BinaryUtils.flipBO(tmpCapturedLen1, PCAP_PACKET_HEADER_CAPLEN_LEN));
                System.arraycopy(captured, pos + PCAP_PACKET_HEADER_WIREDLEN_POS, tmpWiredLen1, 0, 4);
                wiredlen1 = Bytes.toInt(BinaryUtils.flipBO(tmpWiredLen1, 4));
                if (caplen1 > PCAP_PACKET_MIN_LEN && caplen1 < PCAP_PACKET_MAX_LEN && size - pos - 32 - caplen1 > 0) {
                    System.arraycopy(captured, pos + PCAP_PACKET_HEADER_LENGTH + caplen1 + 8, tmpCapturedLen2, 0, PCAP_PACKET_HEADER_CAPLEN_LEN);
                    caplen2 = Bytes.toInt(BinaryUtils.flipBO(tmpCapturedLen2, PCAP_PACKET_HEADER_CAPLEN_LEN));
                    System.arraycopy(captured, pos + PCAP_PACKET_HEADER_LENGTH + caplen1 + PCAP_PACKET_HEADER_WIREDLEN_POS, tmpWiredLen2, 0, 4);
                    wiredlen2 = Bytes.toInt(BinaryUtils.flipBO(tmpWiredLen2, 4));
                    System.arraycopy(captured, pos + PCAP_PACKET_HEADER_LENGTH + caplen1, tmpTimestamp2, 0, PCAP_PACKET_HEADER_TIMESTAMP_LEN);
                    timestamp2 = Bytes.toLong(BinaryUtils.flipBO(tmpTimestamp2, PCAP_PACKET_HEADER_TIMESTAMP_LEN));
                    if (timestamp1 >= minCaptureTime
                            && timestamp1 < maxCaptureTime
                            && minCaptureTime <= timestamp2
                            && timestamp2 < maxCaptureTime
                            && wiredlen1 > PCAP_PACKET_MIN_LEN
                            && wiredlen1 < PCAP_PACKET_MAX_LEN
                            && wiredlen2 > PCAP_PACKET_MIN_LEN
                            && wiredlen2 < PCAP_PACKET_MAX_LEN
                            && caplen1 > 0
                            && caplen1 <= wiredlen1
                            && caplen2 > 0
                            && caplen2 <= wiredlen2
                            && timestamp2 >= timestamp1
                            && timestamp2 - timestamp1 < PCAP_TIMESTAMP_THRESHOLD) {
                        return pos;
                    }
                }
            }
            return pos;
        }
    }

    int readPacket(int packetLen) throws IOException {
        int bufferPosition = PCAP_PACKET_HEADER_LENGTH;
        byte[] tmpBuffer = new byte[packetLen];
        if ((bufferLength = in.read(tmpBuffer)) < packetLen) {
            System.arraycopy(tmpBuffer, 0, buffer, bufferPosition, bufferLength);
            bufferPosition += bufferLength;
            byte[] newPacket = new byte[packetLen - bufferLength];
            if ((bufferLength = in.read(newPacket)) < 0) {
                return bufferPosition;
            }
            System.arraycopy(newPacket, 0, buffer, bufferPosition, bufferLength);
        } else {
            System.arraycopy(tmpBuffer, 0, this.buffer, bufferPosition, this.bufferLength);
        }

        bufferPosition += this.bufferLength;
        return bufferPosition;
    }

    int readPacketHeader() {
        int headerLength;
        byte headerPosn = 0;
        pcapHeader = new byte[PCAP_PACKET_HEADER_LENGTH];
        BytesWritable capturedLen = new BytesWritable();

        try {
            //Читаем хедер
            if ((headerLength = in.read(pcapHeader)) < PCAP_PACKET_HEADER_LENGTH) {
                if (headerLength == -1) {
                    return 0;
                }

                int headerPosn1 = headerPosn + headerLength;
                byte[] e = new byte[PCAP_PACKET_HEADER_LENGTH - headerLength];
                if ((headerLength = in.read(e)) < 0) {
                    consumed = headerPosn1;
                    return -1;
                }
                //Дозаписать остаток хедера
                System.arraycopy(e, 0, pcapHeader, headerPosn1, headerLength);
            }
            //Сливаем все в хедер
//            Нужно ли это?
            //Берем из хедера длину caplen
            capturedLen.set(pcapHeader, 8, PCAP_PACKET_HEADER_CAPLEN_LEN);
            //Копируем в буфер хэдер
            System.arraycopy(pcapHeader, 0, buffer, 0, PCAP_PACKET_HEADER_LENGTH);
        } catch (IOException e) {
            e.printStackTrace();
        }

        return Bytes.toInt(BinaryUtils.flipBO(capturedLen.getBytes(), PCAP_PACKET_HEADER_CAPLEN_LEN));
    }

    public int readFileHeader() {
        try {
            byte[] magicNumber = new byte[4];
            bufferLength = in.read(buffer, 0, PCAP_FILE_HEADER_LENGTH);
            System.arraycopy(buffer, 0, magicNumber, 0, magicNumber.length);
            if (Bytes.toInt(magicNumber) != MAGIC_NUMBER) {
                return 0;
            }
        } catch (IOException e) {
            e.printStackTrace();
        }

        return bufferLength;
    }

    public int readLine(BytesWritable bytes, int maxLineLength, int maxBytesToConsume) throws IOException {
        bytes.set(new BytesWritable());
        boolean hitEndOfFile = false;
        long bytesConsumed = 0L;
        int caplen = readPacketHeader();
        if (caplen == 0) {
            bytesConsumed = 0L;
        } else if (caplen == -1) {
            bytesConsumed += (long) consumed;
        } else if (caplen > 0 && caplen < PCAP_PACKET_MAX_LEN) {
            if ((bufferLength = readPacket(caplen)) < caplen + PCAP_PACKET_HEADER_LENGTH) {
                hitEndOfFile = true;
            }

            bytesConsumed += (long) bufferLength;
            if (!hitEndOfFile) {
                bytes.set(buffer, 0, caplen + PCAP_PACKET_HEADER_LENGTH);
            }
        }
        return (int) Math.min(bytesConsumed, 2147483647L);
    }

    public int readLine(BytesWritable str, int maxLineLength) throws IOException {
        return this.readLine(str, maxLineLength, 2147483647);
    }

    public int readLine(BytesWritable str) throws IOException {
        return this.readLine(str, 2147483647, 2147483647);
    }
}
