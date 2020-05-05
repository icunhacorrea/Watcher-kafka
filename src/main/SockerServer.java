package main;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.net.ServerSocket;
import java.net.Socket;
import recordutil.src.main.Record;


public class SockerServer extends Thread {

    CacheManager cacheManager;

    ServerSocket listener;

    Socket socket;

    public SockerServer(int port, CacheManager cacheManager) throws IOException {
        this.listener = new ServerSocket(port);
        this.cacheManager = cacheManager;
    }

    @Override
    public void run() {
        startServer();
    }

    public void startServer() {
        System.out.println("The watcher server is running...");

        while(true) {
            try {
                socket = listener.accept();
                ObjectInputStream ois = new ObjectInputStream(socket.getInputStream());
                Record record = (Record) ois.readObject();

                cacheManager.insert(record.getOrigem() + ";" + record.getDestino() + ";" + record.getIdSeq(),
                                    record.getTimeStamp() + ";" + record.getQntRecords() + ";" +
                                    record.getKey().toString() + ";" + record.getValue().toString());
                System.out.println("Record Recebido: " + record.toString());

                // Setar quantas mensagens estão vindo do produtor.

                if (record.getIdSeq() == 1) {
                    cacheManager.setTotal(record.getQntRecords());
                }
                //cacheManager.setIdSeq(record.getIdSeq());
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }
}
