package main;

import org.apache.ignite.Ignite;
import org.apache.ignite.lang.IgniteBiTuple;
import org.apache.kafka.clients.producer.*;

import java.util.*;
import java.util.concurrent.TimeUnit;

public class Resender extends Thread {

    CacheManager cacheManager;

    Properties props;

    Producer<String, String> producer;

    int DISPATCH_INTERVAL = 30;
    int SIZE_CACHE_MAX = 100;

    public Resender(CacheManager cacheManager) {
        this.cacheManager = cacheManager;
        this.props = newConfig();
        this.producer = new KafkaProducer<>(props);

    }

    @Override
    public void run() {
        long stop;
        long convert = 0;
        long start = System.nanoTime();
        while(true) {

            if (cacheManager.getTotal() != -1 &&
                    (cacheManager.cacheSize() > SIZE_CACHE_MAX || convert > DISPATCH_INTERVAL)) {
                cacheManager.dispatchList();
            }
            if (cacheManager.getIdSeq() == cacheManager.getTotal()) {
                /*  Entrar nesse laço significa que a produção de mensagens acabou.
                *  1⁰ Despachar últimos recebidos;
                *  2⁰ Reenviar restantes da cache.
                * */
                System.out.println("Produção finalizada, reenviar restantes.");

                // Reinicializar valor de total.
                cacheManager.setTotal(-1);
                cacheManager.setIdSeq(0);
                cacheManager.dispatchList();        // Força despache no que foi recebido.
                reSend();
            }
            stop = System.nanoTime();
            convert = TimeUnit.SECONDS.convert(stop - start, TimeUnit.NANOSECONDS);
            if (convert >  6)
                start = System.nanoTime();
            try {
                Thread.sleep(6000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

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
            IgniteBiTuple<String,String> data = (IgniteBiTuple<String, String>) entry;
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
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ProducerConfig.ACKS_CONFIG, "0");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        return props;
    }
}
