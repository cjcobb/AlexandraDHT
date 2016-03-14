package cj;

import java.math.BigInteger;

/**
 * Created by cj on 3/14/16.
 */
public class NodeLookup {
    BigInteger hash;
    int id;
    String returnIp;
    int returnPort;

    public NodeLookup(BigInteger hash, int id, String returnIp, int returnPort) {
        this.hash = hash;
        this.id = id;
        this.returnIp = returnIp;
        this.returnPort = returnPort;
    }
}
