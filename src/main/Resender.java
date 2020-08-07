package main;

import org.apache.ignite.lang.IgniteBiTuple;
import org.apache.kafka.clients.producer.*;

import java.util.*;
import java.util.concurrent.TimeUnit;

public class Resender extends Thread {

    CacheManager cacheManager;

    Producer<String, String> producer;

    int SIZE_CACHE_MAX = 99;
    int DISPATCH_INTERVAL = 3;

    int TIMEOUT = 60000;

    public Resender(CacheManager cacheManager) {
        this.cacheManager = cacheManager;
        Properties props = newConfig();
        this.producer = new KafkaProducer<>(props);
    }

    @Override
    public void run() {
        long stop;
        long convert = 0;
        long start = System.nanoTime();
        while(true) {
            System.out.println("IdSeq: " + cacheManager.getIdSeq());
            System.out.println("Total: " + cacheManager.getTotal());
	        //System.out.println("Count: " + cacheManager.getCount());
            //System.out.println("Socket: " + cacheManager.getSocketFinish());
            System.out.println("Monitor: " + cacheManager.getMonitorFinish());
            System.out.println("Tamanho da cache: " + cacheManager.cacheSize());
            System.out.println("Tamanho da recived: " + cacheManager.getRecievedSize());
            //System.out.println("To string: " + cacheManager.cacheToString());
            if (mayDispatch(convert)) {
                cacheManager.dispatchList();
            }
	        long stamp = System.currentTimeMillis();
            System.out.println("Stamp: " + (stamp - cacheManager.getTimeout()));
            System.out.println("*******************************************");
            if (finishProduce(stamp)) {
                /*  Entrar nesse laço significa que a produção de mensagens acabou.
                *  1⁰ Despachar últimos recebidos;
                *  2⁰ Reenviar restantes da cache.
                * */
                System.out.println("Produção finalizada, reenviar restantes.");

                // Reinicializar valor de total.
                
                cacheManager.dispatchList();        // Força despache no que foi recebido.
                reSend();
		        cacheManager.setTotal(-1);
                cacheManager.setIdSeq(0);
                cacheManager.stopTimeout();
		        cacheManager.setMonitorFinish(false);
		        cacheManager.removeAll();
                cacheManager.clearRecieved();
		        System.out.println("**************************************************************");
            }
	        stop = System.nanoTime();
            convert = TimeUnit.SECONDS.convert(stop - start, TimeUnit.NANOSECONDS);
            if (convert >  6)
                start = System.nanoTime();
            try {
                Thread.sleep(5000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    @SuppressWarnings("unchecked")
    private void reSend() {
        // Reenviar todos os itens presentes na cache!
        if (cacheManager.cacheSize() == 0) {
            System.out.println("Nenhum elemento precisa ser reenviado.");
            return;
        } else {
            System.out.println("Reenviando " + cacheManager.cacheSize() + " itens não recebidos.");
        }

        Collection<?> collection = cacheManager.getAll();

        collection.forEach(entry -> {
            IgniteBiTuple<String, String> data = (IgniteBiTuple<String, String>) entry;
            String value[] = data.get2().split(";");
            ProducerRecord<String, String> record = new ProducerRecord<>("test-topic",
                                                                         value[2], value[3]);
            try {
                RecordMetadata metadata = producer.send(record).get();
            } catch (Exception e) {
                e.printStackTrace();
            }
        });
        System.out.println("*** Mensagens reenviadas. ***");
        cacheManager.removeAll();
    }

    private static Properties newConfig() {
        Properties props = new Properties();
        //props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "14.0.0.1:9092,14.0.0.3:9092,14.0.0.6:9092");
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        props.put(ProducerConfig.ACKS_CONFIG, "0");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        return props;
    }

    /**
     * Método que define se é possível realizar dispache
     * **/
    private boolean mayDispatch(long convert) {
        return cacheManager.getTotal() != -1 &&
                (cacheManager.cacheSize() > SIZE_CACHE_MAX || convert > DISPATCH_INTERVAL);
    }

    private boolean finishProduce(long stamp) {
        return cacheManager.getMonitorFinish() &&
                (stamp - cacheManager.getTimeout() > TIMEOUT);
    }
}
