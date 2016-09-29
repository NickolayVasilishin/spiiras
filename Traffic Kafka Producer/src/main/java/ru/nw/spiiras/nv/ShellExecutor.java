package ru.nw.spiiras.nv;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

/**
 * Created by Nikolay_Vasilishin on 9/29/2016.
 */
public class ShellExecutor {
    private static final String TCPDUMP = "tcpdump";
    private static final String[] TCPDUMP_ARGS = "-l -n -S".split(" "); //-wU
    private static final String TOPIC = "traffic.simplified";
    private BufferedReader input;
    private BufferedReader error;
    private KafkaTrafficProducer producer;

    public ShellExecutor exec() throws IOException {
        producer = new KafkaTrafficProducer();
        Runtime runtime = Runtime.getRuntime();
        String[] commands = {TCPDUMP, TCPDUMP_ARGS[0], TCPDUMP_ARGS[1], TCPDUMP_ARGS[2]};
        Process proc = runtime.exec(commands);

        input = new BufferedReader(new
                InputStreamReader(proc.getInputStream()));

        error = new BufferedReader(new
                InputStreamReader(proc.getErrorStream()));

// read the output from the command
        String s = null;
        while ((s = input.readLine()) != null) {
            producer.send(TOPIC, s);
        }

// read any errors from the attempted command
        while ((s = error.readLine()) != null) {
            System.err.println(s);
        }
        return this;
    }

    private void close() throws IOException {
        if(error != null) {
            error.close();
        }
        if(input != null) {
            input.close();
        }
        if(producer != null) {
            producer.close();
        }

    }

    public static void main(String[] args) throws IOException {
        ShellExecutor exec = null;
        try {
            exec = new ShellExecutor().exec();
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            if (exec != null) {
                exec.close();
            }
        }
    }
}
