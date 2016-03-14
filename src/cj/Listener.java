package cj;

import java.io.InputStream;
import java.io.OutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.ByteBuffer;

/**
 * Created by cj on 3/7/16.
 */
public class Listener extends Thread {
    int port;
    AlexandraDHTClient me;
    public Listener(int port, AlexandraDHTClient me) {
        this.port = port;
        this.me = me;
    }

    public void run() {
        try {
            ServerSocket serverSocket = new ServerSocket(port);

            while(true) {
                Socket soc = serverSocket.accept();
                InputStream in = soc.getInputStream();
                OutputStream out = soc.getOutputStream();

                CommandCodes command = CommandCodes.intToCommand(in.read());
                if(command == CommandCodes.LOOKUPRESPONSE) {
                    System.out.println("Recevied lookup response");
                    byte[] idBytes = new byte[4];
                    in.read(idBytes);
                    int id = ByteBuffer.wrap(idBytes).getInt();

                    int valLength = in.read();
                    byte[] valBytes = new byte[valLength];
                    in.read(valBytes);
                    String val = new String(valBytes);

                    out.write(1);

                    LookupCallback callback = me.requests.get(id);
                    callback.onResponse(val);
                } else if(command == CommandCodes.NODELOOKUPRESPONSE) {
                    System.out.println("Recevied lookup response");
                    byte[] idBytes = new byte[4];
                    in.read(idBytes);
                    int id = ByteBuffer.wrap(idBytes).getInt();

                    int ipLength = in.read();
                    byte[] ipBytes = new byte[ipLength];
                    in.read(ipBytes);

                    byte[] portBytes = new byte[4];
                    in.read(portBytes);

                    Node node = new Node(new String(ipBytes),ByteBuffer.wrap(portBytes).getInt());
                    out.write(1);

                    NodeLookupCallback callback = me.nodeRequests.get(id);
                    callback.onResponse(node);
                } else {
                    System.out.println("Unrecognized command");
                }
                in.close();
                out.close();
                soc.close();
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
