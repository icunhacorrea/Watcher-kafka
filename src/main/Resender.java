package main;

import org.apache.kafka.clients.producer.*;

import java.util.*;
import java.util.concurrent.TimeUnit;

public class Resender extends Thread {

    Producer<String, String> producer;

    CircularList circularList ;

    int CHECK_SIZE_INTERVAL = 3;

    public Resender(CircularList circularList) {
        Properties props = newConfig();
        this.producer = new KafkaProducer<>(props);
        this.circularList = circularList;
    }

    @Override
    public void run() {
        long stop;
        long convert = 0;
        long start = System.nanoTime();

        while(true) {

            if (circularList.getSizeReceived() > 0) {
                circularList.markReadRecived();
            }

            if (mayChangeSize(convert)) {
                circularList.changeSize();
                printInfo();
                start = System.nanoTime();
            }


            stop = System.nanoTime();
            convert = TimeUnit.SECONDS.convert(stop - start, TimeUnit.NANOSECONDS);

            //printInfo();

            /*try {
                Thread.sleep(5000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }*/
        }
    }

    @SuppressWarnings("unchecked")
    private void reSend() {

    }

    private static Properties newConfig() {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "kafka1:9093,kafka2:9094,kafka3:9095");
        props.put(ProducerConfig.ACKS_CONFIG, "1");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        return props;
    }

    private boolean mayChangeSize(long convert) {
        if (convert > CHECK_SIZE_INTERVAL)
            return true;
        return false;
    }

    public void printInfo() {
        System.out.println("Qnt: " + circularList.getCounter() + " " + circularList.toString());
        System.out.println("Qnt received: " + circularList.getSizeReceived());
        System.out.println("Insertions: " + circularList.getCountInsertions());
        System.out.println("Qnt Read: " + circularList.getQntRead());
        System.out.println("Total esperado: " + circularList.getTotalMesages());
        //circularList.changeSize();
    }

}
