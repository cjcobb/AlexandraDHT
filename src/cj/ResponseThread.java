package cj;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.math.BigInteger;
import java.net.Socket;
import java.nio.ByteBuffer;

/**
 * Created by cj on 3/5/16.
 */
public class ResponseThread extends Thread {

    Socket soc;
    InputStream in;
    CommandCodes command;
    OutputStream out;

    public ResponseThread(Socket soc, InputStream in, OutputStream out,CommandCodes command) {
        this.soc = soc;
        this.in = in;
        this.command = command;
        this.out = out;

    }



    @Override
    public void run() {
        try {
            System.out.println("Handling response in thread");
            if (command == CommandCodes.LOOKUP) {//lookup request
                System.out.println("Received lookup request");

                Lookup request = parseLookup(in);

                BigInteger hash;

                hash = Main.sha1(request.key);
                if (hash.compareTo(Main.me.nodeID) <= 0) {//lookup request
                    //we have the node
                    if (Main.predecessor == null || hash.compareTo(Main.predecessor.nodeID) == 1) {
                        String val = Main.mLocalStore.get(request.key);
                        System.out.println("Value of lookup : " + val);
                        if(val == null) {
                            val = "Not found";
                        }
                        sendResponseToClient(request,val);
                    } else { //predecessor has node
                        System.out.println("Value not owned, asking predecessor");
                        //propagateLookup(request.key,lookupId,Main.predecessor);
                    }
                } else { //successor has node
                    System.out.println("Value not owned, asking successor");
                    //propagateLookup(request.key,lookupId,Main.successor);
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

                Main.mLocalStore.put(key,val);
                System.out.println("Storing (" + key + "," + val + ",");
                out.write(1);

            }
            out.close();
            in.close();
            soc.close();
        } catch (IOException e)
        {
            System.out.println("Exception in response thread");
            e.printStackTrace();
        }
    }

    public void sendResponseToClient(Lookup request, String val) throws IOException {
        Socket soc = new Socket(request.senderIp,request.senderPort);
        InputStream in = soc.getInputStream();
        OutputStream out = soc.getOutputStream();

        out.write(CommandCodes.LOOKUPRESPONSE.ordinal());

        byte[] idBytes = ByteBuffer.allocate(4).putInt(request.id).array();
        out.write(idBytes);

        byte[] valBytes = val.getBytes();
        out.write(valBytes.length);
        out.write(valBytes);

        in.read();
    }

    public static void propagateLookup(String key,int id, AlexandraNode node) throws IOException {
        Socket soc = new Socket(node.ipAddress,node.port);
        InputStream in = soc.getInputStream();
        OutputStream out = soc.getOutputStream();

        out.write(CommandCodes.INTERNALLOOKUP.ordinal());

        byte[] keyBytes = key.getBytes();
        out.write(keyBytes.length);
        out.write(keyBytes);

        ByteBuffer idBuf = ByteBuffer.allocate(4);
        idBuf.putInt(id);
        out.write(idBuf.array());

        String ip = Main.me.ipAddress;
        byte[] ipBytes = ip.getBytes();
        out.write(ipBytes.length);
        out.write(ipBytes);

        int port = Main.me.port;
        ByteBuffer portBuf = ByteBuffer.allocate(4).putInt(port);
        out.write(portBuf.array());

        in.read();

        in.close();
        out.close();
        soc.close();
    }

    public Lookup parseLookup(InputStream in) throws IOException {
        int keyLength = in.read();
        System.out.println("key length is " + keyLength);
        byte[] keyBytes = new byte[keyLength];
        in.read(keyBytes);
        String key = new String(keyBytes);
        System.out.println("key is " + key);

        byte[] idBytes = new byte[4];
        in.read(idBytes);
        int id = ByteBuffer.wrap(idBytes).getInt();
        System.out.println("id is " + id);

        byte[] ipLengthBytes = new byte[4];
        in.read(ipLengthBytes);
        int ipLength = ByteBuffer.allocate(4).wrap(ipLengthBytes).getInt();

        System.out.println("ip length is " + ipLength);

        byte[] ipBytes = new byte[ipLength];
        in.read(ipBytes);
        String ip = new String(ipBytes);

        System.out.println("ip is " + ip);

        byte[] portBytes = new byte[4];
        in.read(portBytes);
        int port = ByteBuffer.allocate(4).wrap(portBytes).getInt();

        System.out.println("port is " + port);


        return new Lookup(key,id,ip,port);
    }

}
