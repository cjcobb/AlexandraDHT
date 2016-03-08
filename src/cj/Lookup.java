package cj;

/**
 * Created by cj on 3/7/16.
 */
public class Lookup {
    String key;
    int id;
    String senderIp;
    int senderPort;

    public Lookup(String key, int id, String senderIp, int senderPort) {
        this.key = key;
        this.id = id;
        this.senderIp = senderIp;
        this.senderPort = senderPort;

    }
}
