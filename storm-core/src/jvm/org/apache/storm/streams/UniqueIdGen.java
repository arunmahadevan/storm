package org.apache.storm.streams;

public class UniqueIdGen {
    private int streamCounter = 0;
    private int spoutCounter = 0;
    private int boltCounter = 0;

    private static final UniqueIdGen instance = new UniqueIdGen();

    public static final UniqueIdGen getInstance() {
        return instance;
    }

    private UniqueIdGen() {
    }

    public String getUniqueStreamId() {
        streamCounter++;
        return "s" + streamCounter;
    }

    public String getUniqueBoltId() {
        boltCounter++;
        return "bolt" + boltCounter;
    }

    public String getUniqueSpoutId() {
        spoutCounter++;
        return "spout" + spoutCounter;
    }

}

