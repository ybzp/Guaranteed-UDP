import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.io.IOException;

/**
 * This class represents a GUDPSocket that implements the GUDPSocketAPI
 * interface.
 * It provides methods for sending and receiving DatagramPackets encapsulated in
 * GUDPPackets.
 * The class maintains state information for each socket address, including a
 * list of packets to be sent,
 * a priority queue of received packets, and a map of received acknowledgements.
 * The class also includes a listener thread for receiving packets and a sender
 * thread for sending packets.
 * 
 * @author Peter
 * @version 1.0
 * @since [1 OCT 2023]
 * @licinfo Apache License 2.0
 */
public class GUDPSocket implements GUDPSocketAPI {
    DatagramSocket datagramSocket;
    private final HashMap<InetSocketAddress, GUDPEndPoint> gudpendpoints = new HashMap<>();
    private final Dictionary<InetSocketAddress, GUDPEndPoint.endPointState> socket_state = new Hashtable<>();
    private final HashMap<InetSocketAddress, LinkedList<GUDPbuffer>> query_sender = new HashMap<>();
    private PriorityQueue<GUDPPacket> receiving = new PriorityQueue<>(Comparator.comparingInt(GUDPPacket::getSeqno));
    private final HashMap<InetSocketAddress, Integer> received_acks = new HashMap<>();
    public final long TIMEOUT = GUDPEndPoint.TIMEOUT_DURATION;
    private volatile boolean isStarted;
    private volatile boolean isReceiving;
    private int sendacksign = 0;
    Timer timer = new Timer();
    SenderThread sendThread = new SenderThread();
    listener_Thread listenThread = new listener_Thread();
    public Map<InetSocketAddress, Integer> Rand = new ConcurrentHashMap<>();
    public int sent = 0;
    public int timercount = 0;

    public GUDPSocket(DatagramSocket socket) {
        datagramSocket = socket;
        isStarted = false;
        isReceiving = false;

    }

    /**
     * Sends a DatagramPacket to the specified address and port.
     * If the socket state for the address is null, initializes a new GUDPEndPoint
     * and generates a BSN packet.
     * Adds the packet to the sending_per_addr list and query_sender map.
     * 
     * @param packet the DatagramPacket to be sent
     * @throws IOException if an I/O error occurs
     */
    public void send(DatagramPacket packet) throws IOException {

        InetSocketAddress sockaddr = new InetSocketAddress(packet.getAddress(), packet.getPort());
        System.out.println("Sending to" + sockaddr);

        if (socket_state.get(sockaddr) == null) {
            if (!gudpendpoints.containsKey(sockaddr)) {
                gudpendpoints.put(sockaddr, new GUDPEndPoint(sockaddr.getAddress(), sockaddr.getPort()));

            }
            if (!Rand.containsKey(sockaddr)) {
                Rand.put(sockaddr, 0);
            }
            ByteBuffer BSN_data = ByteBuffer.allocate(GUDPPacket.HEADER_SIZE);
            BSN_data.order(ByteOrder.BIG_ENDIAN);
            GUDPPacket gudpBSNPacket = new GUDPPacket(BSN_data);
            Random Rand = new Random();
            int BSN_Seqno = Rand.nextInt(1000);
            this.Rand.replace(sockaddr, BSN_Seqno);
            gudpBSNPacket.setSeqno(BSN_Seqno);
            gudpBSNPacket.setType(GUDPPacket.TYPE_BSN);
            gudpBSNPacket.setVersion(GUDPPacket.GUDP_VERSION);
            gudpBSNPacket.setSocketAddress(sockaddr);
            gudpBSNPacket.setPayloadLength(0);
            query_sender.computeIfAbsent(sockaddr, k -> new LinkedList<>()).add(new GUDPbuffer(gudpBSNPacket));
            socket_state.put(sockaddr, GUDPEndPoint.endPointState.INIT);

        }
        this.Rand.replace(sockaddr, this.Rand.get(sockaddr) + 1);

        GUDPPacket gudpdataPacket = GUDPPacket.encapsulate(packet);
        gudpdataPacket.setVersion(GUDPPacket.GUDP_VERSION);
        gudpdataPacket.setSocketAddress(sockaddr);
        gudpdataPacket.setSeqno(this.Rand.get(sockaddr));
        gudpdataPacket.setType(GUDPPacket.TYPE_DATA);
        gudpdataPacket.setPayloadLength(packet.getLength());
        synchronized (query_sender.get(packet.getSocketAddress())) {
            query_sender.computeIfAbsent(sockaddr, k -> new LinkedList<>()).add(new GUDPbuffer(gudpdataPacket));
        }
    }

    /**
     * Receives a DatagramPacket and decapsulates it into a GUDPPacket.
     * If isReceiving is false, initializes the receiving PriorityQueue, sets
     * isReceiving and isStarted to true,
     * starts a listener_Thread and prints "Start".
     * 
     * @param packet the DatagramPacket to be received and decapsulated
     * @throws IOException if an I/O error occurs
     */
    public void receive(DatagramPacket packet) throws IOException {
        if (isReceiving == false) {
            isReceiving = true;
            isStarted = true;
            listener_Thread listenThread = new listener_Thread();
            listenThread.start();
            System.out.println("Start");
        }
        GUDPPacket gudppacket = null;
        synchronized (receiving) {
            if (receiving.size() == 0 || receiving.peek() == null) {
                try {
                    while (receiving.size() == 0 || receiving.peek() == null) {
                        Thread.sleep(TIMEOUT);
                    }
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            } else {

                try {
                    gudppacket = receiving.poll();
                    if (gudppacket.getSocketAddress() != null) {
                        gudppacket.decapsulate(packet);
                        System.out.println("Received packet");
                    }
                    if (gudppacket.getType() == GUDPPacket.TYPE_FIN) {
                        System.out.println("Received FIN packet, terminating");
                        System.exit(0);
                    }
                } catch (NullPointerException e) {
                    System.out.println("Null pointer exception");
                }
            }

        }
    }

    /**
     * Starts the send thread.
     * 
     * @throws IOException if an I/O error occurs.
     */
    public void finish() throws IOException {
        if (isStarted == false) {
            isStarted = true;
            SenderThread sendThread = new SenderThread();
            sendThread.start();
        }

    }

    public void close() throws IOException {
        ;
    }

    /**
     * This class represents a thread that listens for incoming GUDPPackets and
     * processes them accordingly.
     * It continuously listens for incoming packets and adds them to a priority
     * queue for processing.
     * The thread also sends ACK packets for received BSN packets and DATA packets
     * with the correct sequence number.
     * If a FIN packet is received, the thread sends an ACK packet and exits the
     * program.
     * The thread also handles ACK packets by updating the status of queries and
     * cancelling their timers if necessary.
     * Finally, the thread removes processed packets from the priority queue and
     * adds them to a query receiver queue.
     */
    public class listener_Thread extends Thread {

        public void run() {
            while (true) {
                if (receiving == null) {
                    receiving = new PriorityQueue<>(Comparator.comparingInt(GUDPPacket::getSeqno));
                    System.out.println("receiving is null");
                }
                System.out.println("Listening");
                byte[] buf = new byte[GUDPPacket.MAX_DATAGRAM_LEN];
                DatagramPacket udppacket = new DatagramPacket(buf, buf.length);
                isReceiving = true;
                try {
                    datagramSocket.receive(udppacket);
                } catch (IOException e) {
                    e.printStackTrace();
                }
                GUDPPacket gudppacket = null;
                try {
                    gudppacket = GUDPPacket.unpack(udppacket);
                } catch (IOException e) {
                    e.printStackTrace();
                }

                if (gudppacket.getType() == GUDPPacket.TYPE_BSN) {
                    receiving.add(gudppacket);
                    sendacksign = gudppacket.getSeqno() + 1;
                    try {
                        System.out.println("Received BSN");
                        sendack(gudppacket);
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                } else if (gudppacket.getType() == GUDPPacket.TYPE_DATA) {
                    if (gudppacket.getSeqno() == sendacksign) {
                        try {
                            System.out.println("Received DATA");
                            System.out.println("DATA" + gudppacket.getSeqno());
                            sendack(gudppacket);
                        } catch (IOException e) {
                            e.printStackTrace();
                        }
                        sendacksign++;
                        receiving.add(gudppacket);

                    } else if (gudppacket.getSeqno() > sendacksign) {

                        try {
                            gudppacket.setSeqno(sendacksign - 1);
                            sendack(gudppacket);
                        } catch (IOException e) {
                            e.printStackTrace();
                        }

                        System.out.println("Received DATA, loss detected");

                    }
                } else if (gudppacket.getType() == GUDPPacket.TYPE_ACK) {
                    // received_acks.put(gudppacket.getSocketAddress(), gudppacket.getSeqno());
                    System.out.println("Received ACK");
                    if (query_sender.get(gudppacket.getSocketAddress()) != null) {
                        synchronized (query_sender.get(gudppacket.getSocketAddress())) {
                            for (int i = 0; i < query_sender.get(gudppacket.getSocketAddress()).size(); i++) {
                                // System.out.println(query_sender.get(gudppacket.getSocketAddress()).get(i).getGUDPPacket().getSeqno());
                                // System.out.println(gudppacket.getSeqno());
                                if (query_sender.get(gudppacket.getSocketAddress()).get(i).getGUDPPacket()
                                        .getSeqno() < gudppacket.getSeqno()) {
                                    query_sender.get(gudppacket.getSocketAddress()).get(i).setACK_status(true);
                                    System.out.println("ACK status"
                                            + query_sender.get(gudppacket.getSocketAddress()).get(i)
                                                    .getACK_status());

                                    if (query_sender.get(gudppacket.getSocketAddress()).get(i).timer != null) {
                                        query_sender.get(gudppacket.getSocketAddress()).get(i).timer.cancel(); // Cancel
                                                                                                               // the
                                                                                                               // timer
                                        query_sender.get(gudppacket.getSocketAddress()).get(i).timer = null; // Set
                                                                                                             // the
                                                                                                             // timer
                                                                                                             // reference
                                                                                                             // to
                                                                                                             // null
                                    }
                                }
                            }
                        }
                    }
                } else if (gudppacket.getType() == GUDPPacket.TYPE_FIN) {
                    socket_state.put(gudppacket.getSocketAddress(), GUDPEndPoint.endPointState.FINISHED);
                    try {
                        sendack(gudppacket);
                        System.out.println("Received FIN");
                        receiving.add(gudppacket);
                    } catch (IOException e) {
                        e.printStackTrace();
                    }

                }

                // System.out.println("Received" + receiving);

            }

        }

        public void close() throws IOException {
            ;
        }
    }

    /**
     * Sends an acknowledgement packet to the sender of the specified packet.
     * 
     * @param packet the packet to acknowledge
     * @throws IOException if an I/O error occurs while sending the acknowledgement
     *                     packet
     */
    private void sendack(GUDPPacket packet) throws IOException {
        ByteBuffer ack_data = ByteBuffer.allocate(GUDPPacket.HEADER_SIZE);
        ack_data.order(ByteOrder.BIG_ENDIAN);
        GUDPPacket gudpackPacket = new GUDPPacket(ack_data);
        gudpackPacket.setSeqno(packet.getSeqno() + 1);
        gudpackPacket.setType(GUDPPacket.TYPE_ACK);
        gudpackPacket.setVersion(GUDPPacket.GUDP_VERSION);
        gudpackPacket.setSocketAddress(packet.getSocketAddress());
        gudpackPacket.setPayloadLength(0);
        datagramSocket.send(gudpackPacket.pack());
    }

    /**
     * This class represents a thread that sends GUDP packets to the specified
     * address. It removes packets that have been ACKed and sends FIN packet when
     * all packets have been sent.
     * It also sets a timer for each packet and resends the packet if it does not
     * receive an ACK within the timeout period.
     * 
     * @see GUDPbuffer
     * @see GUDPPacket
     */
    public class SenderThread extends Thread {
        public void run() {
            listener_Thread listenThread = new listener_Thread();
            listenThread.start();
            while (isStarted == true) {
                // boolean flag = true;
                for (Map.Entry<InetSocketAddress, LinkedList<GUDPbuffer>> entry : query_sender.entrySet()) {
                    InetSocketAddress address = entry.getKey();
                    // LinkedList<GUDPbuffer> sending_per_addr = query_sender.get(address);
                    InetSocketAddress sockaddr = entry.getKey();

                    // System.out.println("Sending per address" + entry.getKey());
                    // System.out.println("Sending per address" + sockaddr);
                    // System.out.println("Sending per address" + query_sender.get(address));
                    if (query_sender.get(address).size() == 0) {
                        System.exit(0);
                    }
                    while (socket_state.get(entry.getKey()) != GUDPEndPoint.endPointState.FINISHED
                            && query_sender.get(address).size() > 0) {

                        try {
                            int size = query_sender.get(address).size();
                            synchronized (query_sender.get(address)) {

                                for (int j = 0; j < size; j++) {
                                    if (query_sender.get(address).get(j).getACK_status() == true) {
                                        System.out.println("Removing packet " + j + "as ACKed " + j
                                                + query_sender.get(address).get(j).getACK_status());
                                        query_sender.get(address).remove(j);
                                        j--; // Adjust the index after removal
                                        size--; // Adjust the size after removal

                                    }
                                }
                            }
                            System.out.println(query_sender.get(address).size());

                            if (query_sender.get(address).size() == 0) {
                                System.out.println("Sending FIN packet");
                                sendFinpacket(entry.getKey());
                                Thread.sleep(TIMEOUT);
                                socket_state.put(entry.getKey(), GUDPEndPoint.endPointState.FINISHED);
                                break;
                            }
                            for (int i = 0; i < min(3, query_sender.get(address).size()); i++) {
                                if (query_sender.get(address).get(i).timer == null) {
                                    query_sender.get(address).get(i).timer = new Timer();

                                }

                                query_sender.get(address).get(i).timer = new Timer();
                                DatagramPacket udppacket = query_sender.get(address).get(i).getGUDPPacket().pack();
                                udppacket.setSocketAddress(sockaddr);

                                datagramSocket.send(udppacket);

                                query_sender.get(address).get(i).sendCount++;
                                System.out.println("Sending packet" + i + query_sender.get(address).get(i).sendCount);

                                int index = i;
                                System.out.println("Index" + index);
                                if (query_sender.get(address).size() == 0) {
                                    timer.cancel();
                                    break;
                                }
                                GUDPbuffer buffer = query_sender.get(address).get(index);
                                if (buffer == null) {
                                    System.out.println("Buffer at index " + index + " is null.");
                                    continue;
                                }
                                if (buffer.timer == null) {
                                    System.out.println("Timer in buffer at index " + index + " is null.");
                                    continue;
                                }
                                if (buffer.getGUDPPacket() == null) {
                                    System.out.println("GUDPPacket in buffer at index " + index + " is null.");
                                    continue;
                                }

                                query_sender.get(address).get(index).timer.schedule(new TimerTask() {
                                    @Override
                                    public void run() {
                                        synchronized (query_sender.get(address)) {
                                            if (index < query_sender.get(address).size()) {
                                                System.out
                                                        .println("Timer count"
                                                                + query_sender.get(address).get(index).sendCount);
                                                System.out.println("sending+per+addr"
                                                        + query_sender.get(address).get(index).getACK_status());
                                                if (query_sender.get(address).get(index).getACK_status() == false
                                                        && query_sender.get(address)
                                                                .get(index).sendCount < GUDPEndPoint.MAX_RETRY) {
                                                    try {
                                                        DatagramPacket udppacket = query_sender.get(address).get(index)
                                                                .getGUDPPacket().pack();
                                                        udppacket.setSocketAddress(sockaddr);
                                                        datagramSocket.send(udppacket);
                                                        System.out.println("Resending packet" + index);
                                                        query_sender.get(address).get(index).sendCount++;
                                                    } catch (IOException e) {
                                                        e.printStackTrace();
                                                    }
                                                    // } else if (sending_per_addr.get(index).getACK_status() == true
                                                    // || sending_per_addr.get(index) == null) {
                                                    // timer.cancel();
                                                    // timercount--;
                                                } else if (query_sender.get(address)
                                                        .get(index).sendCount >= GUDPEndPoint.MAX_RETRY) {
                                                    System.out.println("Max retry reached");
                                                    timer.cancel();
                                                    System.exit(1);

                                                }
                                            } else {
                                                timer.cancel();
                                            }
                                            // query_sender.get(address).notifyAll();
                                        }
                                    }
                                }, TIMEOUT);

                            }
                            try {
                                Thread.sleep(2 * TIMEOUT);
                            } catch (InterruptedException e) {
                                e.printStackTrace();
                            }
                        }

                        catch (IOException e) {

                            e.printStackTrace();

                        } catch (InterruptedException e) {
                            e.printStackTrace();
                        }

                    }
                    try {
                        Thread.sleep(TIMEOUT);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }
            }
        }

        private int min(int i, int size) {
            if (size < 3) {
                return size;
            } else {
                return 3;
            }
        }

    }

    /**
     * This class represents a buffer for GUDPPackets.
     */
    class GUDPbuffer {
        public GUDPPacket gudppacket;
        public Timer timer = null;
        public boolean isAcked = false;
        public InetSocketAddress address;

        /**
         * Constructor for GUDPbuffer class.
         * 
         * @param gudppacket The GUDPPacket to be stored in the buffer.
         */
        public GUDPbuffer(GUDPPacket gudppacket) {
            this.gudppacket = gudppacket;
        }

        /**
         * Returns the ACK status of the buffer.
         * 
         * @return The ACK status of the buffer.
         */
        public boolean getACK_status() {
            return isAcked;
        }

        public int sendCount = 0;

        /**
         * Sets the ACK status of the buffer.
         * 
         * @param isAcked The ACK status to be set.
         */
        public void setACK_status(boolean isAcked) {
            this.isAcked = isAcked;
        }

        public InetSocketAddress getAddress() {
            return address;
        }

        /**
         * Returns the GUDPPacket stored in the buffer.
         * 
         * @return The GUDPPacket stored in the buffer.
         */
        public GUDPPacket getGUDPPacket() {
            return gudppacket;
        }
    }

    /**
     * Sends a FIN packet to the specified InetSocketAddress.
     * 
     * @param address the InetSocketAddress to send the FIN packet to
     * @throws IOException if an I/O error occurs while sending the packet
     */
    public void sendFinpacket(InetSocketAddress address) throws IOException {
        ByteBuffer fin_data = ByteBuffer.allocate(GUDPPacket.HEADER_SIZE);
        fin_data.order(ByteOrder.BIG_ENDIAN);
        GUDPPacket gudpfinPacket = new GUDPPacket(fin_data);
        gudpfinPacket.setSeqno(this.Rand.get(address) + 1);
        gudpfinPacket.setType(GUDPPacket.TYPE_FIN);
        gudpfinPacket.setVersion(GUDPPacket.GUDP_VERSION);
        gudpfinPacket.setSocketAddress(address);
        gudpfinPacket.setPayloadLength(0);
        datagramSocket.send(gudpfinPacket.pack());
    }

}
