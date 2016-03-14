package cj;

import java.io.IOException;
import java.math.BigInteger;

/**
 * Created by cj on 3/10/16
 */
public class FingerTableThread  extends Thread {

    Node[] fingerTable;
    Node predecessor;
    BigInteger myId;
    AlexandraDHTClient client;
    private class IpAndPort {
        String ip;
        int port;
        public IpAndPort(String ip, int port) {
            this.ip = ip;
            this.port = port;
        }
    }


    public FingerTableThread(
            int numEntries,
            String predIp,
            int predPort,
            BigInteger myId,
            String myIp,
            int myPort,
            int listenerPort) {
        fingerTable = new Node[numEntries];
        this.predecessor = new Node(predIp,predPort);
        client = new AlexandraDHTClient(myIp,myPort,listenerPort);
    }

    @Override
    public void run() {
        while(true) {

            try {
                updateTable();
                Thread.sleep(30 * 1000);
            } catch (InterruptedException e) {

            } catch (IOException e) {
                e.printStackTrace();
            }

        }
    }

    public void updateTable() throws IOException {
        BigInteger inc = BigInteger.ONE;
        for(int i = 0; i < fingerTable.length; i++) {
            final int j = i;
            client.getNodeAsync(myId.add(inc), new NodeLookupCallback() {
                @Override
                public void onResponse(Node node) {
                    fingerTable[j] = node;
                }
            });
            inc = inc.shiftLeft(1);
        }
    }


}
