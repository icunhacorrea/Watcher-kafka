package main;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.Vector;

import recordutil.src.main.Record;

public class SocketServer extends Thread {

    ServerSocket listener;

    Socket socket;

    CircularList circularList;

    public SocketServer(int port, CircularList circularList) throws IOException {
        this.listener = new ServerSocket(port);
        this.circularList = circularList;
    }

    @Override
    public void run() {
        startServer();
    }

    @SuppressWarnings("unchecked")
    public void startServer() {
        System.out.println("The watcher socker server is running...");
        try {
            listener.setReceiveBufferSize(Integer.MAX_VALUE);

            int count = 0;

            while(true) {
                socket = listener.accept();
                ObjectInputStream ois = new ObjectInputStream(socket.getInputStream());
                Vector<Record> records = (Vector<Record>) ois.readObject();

                for (Record record : records) {
                    count++;
                    //System.out.println("Record Recebido: " + record.toString());
                    synchronized (circularList) {

                        circularList.insert(record, record.getOrigem() + ";" +
                                    record.getDestino() + ";" + record.getIdSeq() + ";" + record.getQntRecords());

                        if (circularList.getTotalMesages() == -1)
                            circularList.setTotalMesages(record.getQntRecords());
                    }
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
