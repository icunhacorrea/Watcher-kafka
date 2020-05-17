package main;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.Vector;

import recordutil.src.main.Record;


public class SocketServer extends Thread {

    CacheManager cacheManager;

    ServerSocket listener;

    Socket socket;

    public SocketServer(int port, CacheManager cacheManager) throws IOException {
        this.listener = new ServerSocket(port);
        this.cacheManager = cacheManager;
    }

    @Override
    public void run() {
        startServer();
    }

    @SuppressWarnings("unchecked")
    public void startServer() {
        System.out.println("The watcher server is running...");
        try {
            listener.setReceiveBufferSize(Integer.MAX_VALUE);

            while(true) {
                socket = listener.accept();
                ObjectInputStream ois = new ObjectInputStream(socket.getInputStream());
                Vector<Record> records = (Vector<Record>) ois.readObject();

                for (Record record : records) {
		    //System.out.println("Record Recebido: " + record.toString());
                    cacheManager.insert(record.getOrigem() + ";" + record.getDestino() + ";" +
                            record.getIdSeq(), record.getTimeStamp() + ";" + record.getQntRecords() + ";" +
                            record.getKey().toString() + ";" + record.getValue().toString());
                    if (record.getIdSeq() == 1) {
			System.out.println("Iniciando nova produção.");
                        cacheManager.setTotal(record.getQntRecords());
                    }
		   //cacheManager.setIdSeq(record.getIdSeq());
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}

