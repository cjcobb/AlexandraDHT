package cj;

import java.math.BigInteger;

/**
 * Created by cj on 3/2/16.
 */
public class AlexandraNode {
    BigInteger nodeID;
    String ipAddress;
    int port;

    public AlexandraNode(BigInteger nodeID, String ipAddress, int port) {
        this.nodeID = nodeID;
        this.ipAddress = ipAddress;
        this.port = port;
    }
}
