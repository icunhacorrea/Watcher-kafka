package main;

import org.apache.kafka.clients.producer.*;

import java.util.*;
import java.util.concurrent.TimeUnit;

public class Controller extends Thread {

    CircularList circularList ;

    int CHECK_SIZE_INTERVAL = 3;

    public Controller(CircularList circularList) {
        this.circularList = circularList;
    }

    @Override
    public void run() {
        long stop;
        long convert = 0;
        long start = System.nanoTime();

        while(true) {

            if (circularList.getSizeReceived() != 0) {
                circularList.markReadRecived();
            }

            if (mayChangeSize(convert)) {
                printInfo();
                circularList.changeSize();
                start = System.nanoTime();
            }

            if (circularList.getCountInsertions() == circularList.getTotalMesages()) {
                // start timeout
                System.out.println("Produção de mensagens acabou, inicializar timeout para varrer lista.");
            }

            stop = System.nanoTime();
            convert = TimeUnit.SECONDS.convert(stop - start, TimeUnit.NANOSECONDS);

        }
    }

    private boolean mayChangeSize(long convert) {
        if (convert > CHECK_SIZE_INTERVAL)
            return true;
        return false;
    }

    public void printInfo() {
        //System.out.println("Qnt: " + circularList.getCounter() + " " + circularList.toString());
        System.out.println("Qnt received: " + circularList.getSizeReceived());
        System.out.println("Insertions: " + circularList.getCountInsertions());
        System.out.println("Qnt Read: " + circularList.getQntRead());
        System.out.println("Total esperado: " + circularList.getTotalMesages());
        //circularList.changeSize();
    }

}
