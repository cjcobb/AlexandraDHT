package cj;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.math.BigInteger;
import java.net.Inet4Address;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.security.NoSuchAlgorithmException;

/**
 * Created by cj on 3/2/16.
 */
public class Client {

    public static void main(String[] args) {
        String key = "foo";
        String val = "bar";
        String ip = "localhost";
        int port = 3001;
        int portToUse = 3003;

        try {
            AlexandraDHTClient client = new AlexandraDHTClient(ip,port,portToUse);
            client.getAsync("CJ", new LookupCallback() {
                @Override
                public void onResponse(String key) {
                    System.out.println("key: " + key);
                }
            });

            client.put("CJ","Alex");
            client.getAsync("CJ", new LookupCallback() {
                @Override
                public void onResponse(String key) {
                    System.out.println("key is" + key);
                }
            });
            try {
                Main.setUpHash("SHA1");
            } catch (NoSuchAlgorithmException e) {
                e.printStackTrace();
            }
            client.getNodeAsync(Main.getRandomHash(), new NodeLookupCallback() {
                @Override
                public void onResponse(Node node) {
                    System.out.println("Node ip is " + node.ip);
                    System.out.println("Node port is " + node.port);
                }
            });
        } catch (IOException e) {
            e.printStackTrace();
        }


    }


    public static AlexandraNode join(String ipAddress,int destPort,int portToUse) throws IOException
    {
        Socket soc = new Socket(ipAddress,destPort);
        InputStream in = soc.getInputStream();
        OutputStream out = soc.getOutputStream();

        ByteBuffer buf = ByteBuffer.allocate(4);
        buf.putInt(portToUse);
        out.write(CommandCodes.JOIN.ordinal());
        out.write(buf.array());

        int acceptOrReject = in.read();
        if(acceptOrReject != 1 ) {
            return null;
        }
        int length = in.read();
        byte[] nodeIdBytes = new byte[length];
        in.read(nodeIdBytes);

        BigInteger nodeId = new BigInteger(1,nodeIdBytes);

        String myIp = Inet4Address.getLocalHost().getHostAddress();
        System.out.println("received nodeId is " + nodeId);
        return new AlexandraNode(nodeId,myIp,portToUse);

    }

    public static void list(String ipAddress, int port) throws IOException
    {
        Socket soc = new Socket(ipAddress,port);
        InputStream in = soc.getInputStream();
        OutputStream out = soc.getOutputStream();
        out.write(CommandCodes.LIST.ordinal());
        int c;
        while((c = in.read())!=-1) {
            System.out.print((char) c);
        }
        return;
    }

    public static String lookup(String ipAddress, int port, String key) throws IOException
    {
        Socket soc = new Socket(ipAddress,port);
        InputStream in = soc.getInputStream();
        OutputStream out = soc.getOutputStream();

        out.write(0);
        out.write(key.length());
        out.write(key.getBytes());

        int c = -1;

        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        while((c = in.read()) != -1) {
            baos.write(c);
        }

        in.close();
        out.close();
        soc.close();

        return new String(baos.toByteArray());

    }

    public static void store(String key, String val) throws IOException
    {
        Socket soc = new Socket("localhost",3001);
        InputStream in = soc.getInputStream();
        OutputStream out = soc.getOutputStream();

        out.write(1);
        out.write(key.length());
        out.write(key.getBytes());

        out.write(val.length());
        out.write(val.getBytes());

        in.close();
        out.close();
        soc.close();
    }

    public static void sendClear(String ipAddress, int port) throws IOException {
        Socket soc = new Socket(ipAddress,port);
        InputStream in = soc.getInputStream();
        OutputStream out = soc.getOutputStream();

        out.write(CommandCodes.CLEAR.ordinal());
        in.read();
        in.close();
        out.close();
        soc.close();
    }

    public static void sendUpdateSuccessor(String destIp,
                                           int destPort,
                                           String newSuccIp,
                                           int newSuccPort,
                                           BigInteger nodeId) throws IOException
    {
        Socket soc = new Socket(destIp,destPort);
        InputStream in = soc.getInputStream();
        OutputStream out = soc.getOutputStream();

        out.write(3);
        byte[] ipBytes = newSuccIp.getBytes();
        out.write(ipBytes.length);
        ByteBuffer buf = ByteBuffer.allocate(4);
        buf.putInt(newSuccPort);
        out.write(buf.array());
        byte[] nodeIdBytes = nodeId.toByteArray();
        out.write(nodeIdBytes.length);
        out.write(nodeIdBytes);
    }
}
