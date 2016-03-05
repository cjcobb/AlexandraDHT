package cj;

import com.sun.tools.doclets.internal.toolkit.util.DocFinder;
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
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class Main {

    static HashMap<String, String> mLocalStore = new HashMap<>();
    static MessageDigest mDigest;

    /*
    TODO: join and persistent storage over filesystem
     */

    static AlexandraNode predecessor;
    static AlexandraNode successor;
    static AlexandraNode me;

    public static void main(String[] args) {
        //Start
        System.out.println("Joining distributed hash table...");
        String algorithm = "SHA1";
        try {
            setUpHash(algorithm);
        } catch (NoSuchAlgorithmException e) {
            System.out.println(algorithm + " could not be used. Exiting");
            e.printStackTrace();
            return;
        }
        try {

            String ipAddress = Inet4Address.getLocalHost().getHostAddress();
            int port = Integer.parseInt(args[0]);
            me = new AlexandraNode(maxHash(), ipAddress, port);
            System.out.println("My node id is " + maxHash());

            if(args.length > 1) {
                String ipToJoin = args[1];
                int portToJoin = Integer.parseInt(args[2]);
            }
            serverLoop(port);
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

                int length = in.read();
                byte[] keyBytes = new byte[length];
                in.read(keyBytes);

                String key = new String(keyBytes);
                System.out.println("Looking up key : " + key);

                BigInteger hash;

                hash = sha1(key);
                if (hash.compareTo(me.nodeID) <= 0) {//lookup request
                    //we have the node
                    if (predecessor == null || hash.compareTo(predecessor.nodeID) == 1) {
                        String val = lookup(key);
                        System.out.println("Value of lookup : " + val);
                        out.write(val.getBytes());
                    } else { //predecessor has node
                        System.out.println("Value not owned, asking predecessor");
                        String val = lookupRemote(predecessor.ipAddress, predecessor.port, key);
                        System.out.println("Value of remote lookup from predecessor : " + val);
                        out.write(val.getBytes());
                    }
                } else { //successor has node
                    System.out.println("Value not owned, asking successor");
                    String val = lookupRemote(successor.ipAddress, successor.port, key);
                    System.out.println("Value of remote lookup from successor: " + val);
                    out.write(val.getBytes());
                }


            } else if (command == CommandCodes.STORE) {//store request
                System.out.println("Received store request");
                int keyLength = in.read();
                byte[] keyBytes = new byte[keyLength];
                in.read(keyBytes);

                int valLength = in.read();
                byte[] valBytes = new byte[valLength];
                in.read(valBytes);

                String key = new String(keyBytes);
                String val = new String(valBytes);

                store(key, val);
                System.out.println("Storing (" + key + "," + val + ",");
                out.write(1);

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

                //TODO: this returns localhost
                String remoteIp = soc.getInetAddress().getHostAddress();
                System.out.println("Remote node attempting to join via ip : " + remoteIp + " and port : " + remotePort);
                try {
                    Socket testSoc = new Socket(remoteIp, remotePort);
                    InputStream testIn = testSoc.getInputStream();
                    OutputStream testOut = testSoc.getOutputStream();
                    testOut.write(CommandCodes.ALIVE.ordinal());
                    int response = testIn.read();
                    testSoc.close();
                    if (CommandCodes.intToCommand(response) == CommandCodes.ALIVE) {
                        out.write(1);
                        System.out.println("Remote node joining via ip : " + remoteIp + " and port : " + remotePort);

                        BigInteger predId = predecessor != null ? predecessor.nodeID : BigInteger.ZERO;
                        BigInteger newId = predId.add(me.nodeID.subtract(predId).shiftRight(1));

                        System.out.println("New nodes id is " + newId);

                        AlexandraNode newPred = new AlexandraNode(newId, remoteIp, remotePort);
                        byte[] idBytes = newId.toByteArray();
                        out.write(idBytes.length);
                        out.write(idBytes);

                        Socket newNodeSoc = new Socket(remoteIp,remotePort);
                        InputStream newNodeIn = newNodeSoc.getInputStream();
                        OutputStream newNodeOut = newNodeSoc.getOutputStream();
                        newNodeOut.write(CommandCodes.UPDATENODEID.ordinal());
                        newNodeOut.write(idBytes.length);
                        newNodeOut.write(idBytes);
                        System.out.println("sent new node id to new node");
                        newNodeIn.read();
                        System.out.println("read ack");
                        newNodeIn.close();
                        newNodeOut.close();
                        newNodeSoc.close();
                        if (predecessor != null) {
                            Client.sendUpdateSuccessor(predecessor.ipAddress, predecessor.port, remoteIp, remotePort, newId);
                        }
                        predecessor = newPred;
                        System.out.println("rehashing");
                        rehashAndSend(newPred);
                        System.out.println("done rehashing");
                    } else {
                        out.write(-1);
                    }
                } catch (IOException e) {
                    System.out.println("Remote node not actually running dht on proposed socket");
                    e.printStackTrace();
                }

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
            } else if(command == CommandCodes.ALIVE) {
                System.out.println("received Alive request");
                out.write(CommandCodes.ALIVE.ordinal());
            } else if(command == CommandCodes.UPDATENODEID) {
              System.out.println("received update node id request");
                int length = in.read();
                byte[] newNodeId = new byte[length];
                in.read(newNodeId);
                me.nodeID = new BigInteger(newNodeId);
                System.out.println("updated node id");
                out.write(1);
            } else {
                System.out.println("unrecognized command");
                out.write("error: unrecognized command. terminating stream".getBytes());
            }
            out.close();
            in.close();
            soc.close();
        }
    }

    public static void rehashAndSend(AlexandraNode node) throws IOException {
        Socket soc = new Socket(node.ipAddress, node.port);
        InputStream in = soc.getInputStream();
        OutputStream out = soc.getOutputStream();
        System.out.println("sending transfer command");
        out.write(CommandCodes.TRANSFER.ordinal());
        System.out.println("send ordinal command");


        Set<String> keysToRemove = new HashSet<>();

        for (Map.Entry entry : mLocalStore.entrySet()) {
            String key = (String) entry.getKey();
            String value = (String) entry.getValue();

            if (sha1(key).compareTo(node.nodeID) <= 0) {
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
        in.close();
        out.close();
        soc.close();
    }

    public static String lookupRemote(String ipAddress, int port, String key) throws IOException {
        return Client.lookup(ipAddress, port, key);
    }

    //hashes a key to a big int
    public static BigInteger sha1(String str) {

        byte[] result = mDigest.digest(str.getBytes());
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

}
