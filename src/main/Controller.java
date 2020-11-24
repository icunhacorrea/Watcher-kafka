package main;

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

            if (mayChangeSize(convert)) {
                circularList.changeSize();
                start = System.nanoTime();
            }

            if (circularList.getSizeReceived() != 0) {
                circularList.markReadRecived();
            }

            if (mayFinish()) {
                // start timeout
                if (circularList.getTimeout() == Long.MIN_VALUE) {
                    circularList.startTimeout();
                    System.out.println("Produção de mensagens acabou, inicializar timeout para varrer lista.");
                }
            }

            stop = System.nanoTime();
            convert = TimeUnit.SECONDS.convert(stop - start, TimeUnit.NANOSECONDS);

        }
    }

    private boolean mayFinish() {
        return (circularList.getCountInsertions() == circularList.getTotalMesages()) &&
                (circularList.getSizeReceived() == 0);
    }

    private boolean mayChangeSize(long convert) {
        if (convert > CHECK_SIZE_INTERVAL)
            return true;
        return false;
    }
}
