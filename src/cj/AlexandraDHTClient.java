package cj;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.math.BigInteger;
import java.net.Inet4Address;
import java.net.Socket;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Created by cj on 3/7/16.
 */
public class AlexandraDHTClient {

    String contactIp;
    int contactPort;
    ConcurrentHashMap<Integer,LookupCallback> requests;
    ConcurrentHashMap<Integer,NodeLookupCallback> nodeRequests;
    int requestIdCounter = 0;
    int listenerPort;
    String listenerIp;
    Listener listener;

    public AlexandraDHTClient(String contactIp,int contactPort, int listenerPort) {
        this.contactIp = contactIp;
        this.contactPort = contactPort;
        this.requests = new ConcurrentHashMap<>();
        this.nodeRequests = new ConcurrentHashMap<>();
        this.listenerPort = listenerPort;
        try {
            this.listenerIp = Inet4Address.getLocalHost().getHostAddress();
            this.listener = new Listener(listenerPort,this);
        } catch (UnknownHostException e) {
            System.out.println("Couldnt start listener");
            e.printStackTrace();
        }
        this.listener.start();
    }


    public void getAsync(String key, LookupCallback callback) throws IOException {
        int id = getNewRequestId();
        requests.put(id,callback);

        Socket soc = new Socket(contactIp,contactPort);
        InputStream in = soc.getInputStream();
        OutputStream out = soc.getOutputStream();

        byte[] keyBytes = key.getBytes();
        out.write(CommandCodes.LOOKUP.ordinal());
        out.write(keyBytes.length);
        out.write(keyBytes);

        byte[] idBytes = ByteBuffer.allocate(4).putInt(id).array();
        out.write(idBytes);

        String ip = listenerIp;
        byte[] ipBytes = ip.getBytes();

        byte[] ipLengthBytes = ByteBuffer.allocate(4).putInt(ipBytes.length).array();
        out.write(ipLengthBytes);
        out.write(ipBytes);

        int port = listenerPort;
        ByteBuffer portBuf = ByteBuffer.allocate(4).putInt(port);
        out.write(portBuf.array());



        in.read();

        in.close();
        out.close();
        soc.close();
    }

    public void getNodeAsync(BigInteger hash, NodeLookupCallback callback) throws IOException {
        int id = getNewRequestId();
        nodeRequests.put(id,callback);

        Socket soc = new Socket(contactIp,contactPort);
        InputStream in = soc.getInputStream();
        OutputStream out = soc.getOutputStream();

        byte[] hashBytes = hash.toByteArray();
        out.write(CommandCodes.NODELOOKUP.ordinal());
        out.write(hashBytes.length);
        out.write(hashBytes);

        byte[] idBytes = ByteBuffer.allocate(4).putInt(id).array();
        out.write(idBytes);

        String ip = listenerIp;
        byte[] ipBytes = ip.getBytes();
        byte[] ipLengthBytes = ByteBuffer.allocate(4).putInt(ipBytes.length).array();
        out.write(ipLengthBytes);
        out.write(ipBytes);

        int port = listenerPort;
        ByteBuffer portBuf = ByteBuffer.allocate(4).putInt(port);
        out.write(portBuf.array());



        in.read();

        in.close();
        out.close();
        soc.close();
    }

    public void put(String key, String val) throws IOException {
        Socket soc = new Socket(this.contactIp,this.contactPort);
        InputStream in = soc.getInputStream();
        OutputStream out = soc.getOutputStream();

        out.write(1);
        out.write(key.length());
        out.write(key.getBytes());

        out.write(val.length());
        out.write(val.getBytes());

        in.read();

        in.close();
        out.close();
        soc.close();
    }

    public int getNewRequestId() {
        return ++requestIdCounter;
    }
}
