package ru.nw.spiiras.nv;

/**
 * Created by Nikolay_Vasilishin on 9/29/2016.
 */
public interface TrafficProvider {
    boolean hasNext();
    TrafficContainer getNext();
}
