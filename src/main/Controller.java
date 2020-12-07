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

            if (circularList.getSizeReceived() != 0) {
                //circularList.markReadRecived();
                circularList.markReadRecived2();
            }

            if (mayChangeSize(convert)) {
                circularList.changeSize();
                start = System.nanoTime();
            }

            if (mayFinish()) {
                // start timeout
                if (circularList.getTimeout() == 0 && circularList.getTotalMesages() != 0) {
                    circularList.startTimeout();
                    System.out.println("Produção de mensagens acabou, inicializar timeout para varrer lista.");
                }
            }

            stop = System.nanoTime();
            convert = TimeUnit.SECONDS.convert(stop - start, TimeUnit.NANOSECONDS);

        }
    }

    private boolean mayFinish() {
        return (circularList.getCountInsertions() == circularList.getTotalMesages());
    }

    private boolean mayChangeSize(long convert) {
        if (convert > CHECK_SIZE_INTERVAL)
            return true;
        return false;
    }
}
