package cj;

import com.sun.tools.doclets.internal.toolkit.util.DocFinder;
import com.sun.tools.internal.ws.processor.model.Response;
import com.sun.xml.internal.ws.encoding.MtomCodec;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.math.BigInteger;
import java.net.ConnectException;
import java.net.Inet4Address;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

public class Main {

    static ConcurrentHashMap<String, String> mLocalStore = new ConcurrentHashMap<>();
    static MessageDigest mDigest;

    /*
    TODO: join and persistent storage over filesystem
     */

    static AlexandraNode predecessor;
    static AlexandraNode successor;
    static AlexandraNode me;

    static int lookupCounter = 0;

    static AlexandraDHTClient client;
    static String myIp;
    static int myPort;
    static int myListenerPort;

    public static void main(String[] args) {
        //Start
        System.out.println("Joining distributed hash table...");
        String algorithm = "SHA1";

        System.out.println("Date is " + new Date().getTime());
        try {
            setUpHash(algorithm);
        } catch (NoSuchAlgorithmException e) {
            System.out.println(algorithm + " could not be used. Exiting");
            e.printStackTrace();
            return;
        }
        try {

            myIp = Inet4Address.getLocalHost().getHostAddress();
            myPort = Integer.parseInt(args[0]);
            myListenerPort = Integer.parseInt(args[1]);


            if(args.length > 2) {
                String contactIp = args[2];
                int contactPort = Integer.parseInt(args[3]);
                BigInteger myId = getRandomHash();
                me = new AlexandraNode(myId,myIp,myPort);
                client = new AlexandraDHTClient(contactIp,contactPort,myListenerPort);

                client.getNodeAsync(myId, new NodeLookupCallback() {
                    @Override
                    public void onResponse(Node node) {
                        try {
                            join(node.ip, node.port);
                        } catch(IOException e) {
                            System.out.println("Error joining");
                            e.printStackTrace();
                        }
                    }
                });

            } else {
                me = new AlexandraNode(maxHash(), myIp, myPort);
                System.out.println("My node id is " + maxHash());
            }
            serverLoop(myPort);
        } catch (IOException e) {
            e.printStackTrace();
            System.out.println("error joining distributed hash table");
        }
    }

    public static void setUpHash(String algorithm) throws NoSuchAlgorithmException {
        mDigest = MessageDigest.getInstance(algorithm);
    }

    public static BigInteger maxHash() {
        byte[] bytes = new byte[20];
        for(int i = 0; i < 20; i++) {
            bytes[i] = (byte) 0xFF;
        }
        return new BigInteger(1,bytes);
    }

    public static BigInteger getRandomHash() {
        Date now = new Date();
        double rand = Math.random();
        return sha1((now.toString() + rand));
    }

    /*
    Protocol: Send 0 for lookup, send 1 for store, 2 for join
    send length of key. Then send key. Wait for response
     */
    public static void serverLoop(int portToUse) throws IOException {
        ServerSocket serverSoc = new ServerSocket(portToUse);
        System.out.println("Ready to accept lookups on port " + portToUse);
        while (true) {
            Socket soc = serverSoc.accept();

            InputStream in = soc.getInputStream();
            OutputStream out = soc.getOutputStream();
            CommandCodes command = CommandCodes.intToCommand(in.read());

            System.out.println("recevied something");


            if (command == CommandCodes.LOOKUP) {//lookup request
                System.out.println("Received lookup request");
                ResponseThread response = new ResponseThread(soc,in,out,command);
                response.start();
            } else if (command == CommandCodes.STORE) {//store request
                System.out.println("Received store request");
                ResponseThread response = new ResponseThread(soc,in,out,command);
                response.start();

            } else if(command == CommandCodes.NODELOOKUP) {
                System.out.println("Received store request");
                ResponseThread response = new ResponseThread(soc,in,out,command);
                response.start();
            } else if(command == CommandCodes.INTERNALLOOKUPRESPONSE) {
                System.out.println("Recevied lookup response");
                ResponseThread response = new ResponseThread(soc,in,out,command);
                response.start();
            } else if (command == CommandCodes.JOIN) {//join request
                //when getting a join request, we simply take half of the range
                //owned by this node and give it to the new node
                //we then need to inform the old predecessor of this update

                System.out.println("Received join request");

                //the port is sent as 4 bytes, always
                byte[] portBytes = new byte[4];
                in.read(portBytes);
                ByteBuffer buf = ByteBuffer.wrap(portBytes);
                int remotePort = buf.getInt();

                int idLength = in.read();
                byte[] idBytes = new byte[idLength];
                in.read(idBytes);

                rehashAndSend(in,out,new BigInteger(idBytes));

                close(soc,in,out);
            } else if (command == CommandCodes.UPDATESUCCESSOR) {//update successor request
                System.out.println("Received update command");
                int ipLength = in.read();
                byte[] ipBytes = new byte[ipLength];
                in.read(ipBytes);
                byte[] portBytes = new byte[4];
                in.read(portBytes);
                int nodeIdLength = in.read();
                byte[] nodeIdBytes = new byte[nodeIdLength];
                in.read(nodeIdBytes);
                BigInteger newNodeId = new BigInteger(1, nodeIdBytes);

                successor = new AlexandraNode(newNodeId, new String(ipBytes), ByteBuffer.wrap(portBytes).getInt());

                close(soc,in,out);
            } else if(command == CommandCodes.TRANSFER) {
                System.out.println("Received transfer command");
                int c = 0;
                while((c = in.read())!= 0){
                    System.out.println("reading value");
                    int keyLength = in.read();
                    byte[] keyBytes = new byte[keyLength];
                    in.read(keyBytes);
                    System.out.println("key is " + new String(keyBytes));
                    int valueLength = in.read();
                    byte[] valueBytes = new byte[valueLength];
                    in.read(valueBytes);
                    System.out.println("value is " + new String(valueBytes));

                    mLocalStore.put(new String(keyBytes),new String(valueBytes));
                }
                out.write(1);
                close(soc,in,out);
            } else if(command == CommandCodes.LIST) {
                System.out.println("Received list command");
                for(Map.Entry entry : mLocalStore.entrySet()) {
                    String key = (String) entry.getKey();
                    System.out.println(key);
                    String value = (String) entry.getValue();
                    out.write(key.getBytes());
                    out.write(", ".getBytes());
                    out.write(value.getBytes());
                    out.write('\n');
                }
                close(soc,in,out);
            } else if(command == CommandCodes.ALIVE) {
                System.out.println("received Alive request");
                out.write(CommandCodes.ALIVE.ordinal());
                close(soc,in,out);
            } else if(command == CommandCodes.UPDATENODEID) {
              System.out.println("received update node id request");
                int length = in.read();
                byte[] newNodeId = new byte[length];
                in.read(newNodeId);
                me.nodeID = new BigInteger(newNodeId);
                System.out.println("updated node id");
                out.write(1);
                close(soc,in,out);
            } else if(command == CommandCodes.CLEAR) {
                mLocalStore.clear();
                out.write(1);
                if(predecessor!= null) {
                    Client.sendClear(predecessor.ipAddress, predecessor.port);
                }
                close(soc,in,out);
            } else {
                System.out.println("unrecognized command");
                out.write("error: unrecognized command. terminating stream".getBytes());
                close(soc,in,out);
            }

        }
    }

    public static void close(Socket soc, InputStream in, OutputStream out) throws IOException
    {
        out.close();
        in.close();
        soc.close();
    }

    public static void rehashAndSend(InputStream in, OutputStream out, BigInteger hash) throws IOException {
        Set<String> keysToRemove = new HashSet<>();

        for (Map.Entry entry : mLocalStore.entrySet()) {
            String key = (String) entry.getKey();
            String value = (String) entry.getValue();

            if (sha1(key).compareTo(hash) <= 0) {
                System.out.println("sending entry");
                out.write(1);
                out.write(key.length());
                out.write(key.getBytes());
                out.write(value.length());
                out.write(value.getBytes());
                keysToRemove.add(key);
                System.out.println("sent entry");
            }
        }
        System.out.println("sending terminate");
        out.write(0);
        System.out.println("sent terminate");


        for(String key : keysToRemove) {
            mLocalStore.remove(key);
        }
        in.read();
        System.out.println("received ack");
    }

    public static String lookupRemote(String ipAddress, int port, String key) throws IOException {
        return Client.lookup(ipAddress, port, key);
    }

    //hashes a key to a big int
    public static BigInteger sha1(String str) {

        byte[] input = str.getBytes();

        byte[] result = mDigest.digest(input);
        return new BigInteger(1,result);
    }

    public static String lookup(String key) {
        String val = mLocalStore.get(key);
        if (val == null) {
            return "Error, value not found";
        }
        return val;
    }

    public static void store(String key, String val) {
        mLocalStore.put(key, val);
    }

    public static int getNewLookupId() {
        int ret = ++lookupCounter;
        if(ret == 0) {
            return ++lookupCounter;
        }
        else {
            return lookupCounter;
        }
    }

    public static void join(String destIp,int destPort) throws IOException
    {
        Socket soc = new Socket(destIp,destPort);
        InputStream in = soc.getInputStream();
        OutputStream out = soc.getOutputStream();

        ByteBuffer buf = ByteBuffer.allocate(4);
        buf.putInt(myPort);
        out.write(CommandCodes.JOIN.ordinal());
        out.write(buf.array());

        byte[] idBytes = me.nodeID.toByteArray();
        out.write(idBytes.length);
        out.write(idBytes);

        int c = 0;
        while((c = in.read())!= 0){
            System.out.println("reading value");
            int keyLength = in.read();
            byte[] keyBytes = new byte[keyLength];
            in.read(keyBytes);
            System.out.println("key is " + new String(keyBytes));
            int valueLength = in.read();
            byte[] valueBytes = new byte[valueLength];
            in.read(valueBytes);
            System.out.println("value is " + new String(valueBytes));

            mLocalStore.put(new String(keyBytes),new String(valueBytes));
        }
        out.write(1);



    }

}
