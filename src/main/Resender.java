package main;

import org.apache.kafka.clients.producer.*;

import java.util.*;
import java.util.concurrent.TimeUnit;

public class Resender extends Thread {

    Producer<String, String> producer;

    CircularList circularList ;

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

            System.out.println("Qnt: " + circularList.getCounter() + " " + circularList.toString());
            //System.out.println("Qnt received: " + circularList.getSizeReceived());

            synchronized (circularList) {
                if (circularList.getSizeReceived() > 0)
                    circularList.markRead();
            }

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
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ProducerConfig.ACKS_CONFIG, "1");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        return props;
    }

    /**
     * Método que define se é possível realizar dispache
     * **/
    private boolean mayDispatch(long convert) {
        return true;
    }

}
