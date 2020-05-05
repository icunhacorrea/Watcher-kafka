package main;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;

import java.util.Properties;
import java.util.concurrent.TimeUnit;

public class Resender extends Thread {

    CacheManager cacheManager;

    Properties props;

    Producer<String, String> producer;

    public Resender(CacheManager cacheManager) {
        this.cacheManager = cacheManager;
        this.props = newConfig();
        this.producer = new KafkaProducer<>(props);
    }

    @Override
    public void run() {
        long start = System.nanoTime();
        long stop;
        long convert = 0;
        while(true) {
            if (cacheManager.getTotal() != -1 &&
                    (cacheManager.cacheSize() > 5) || convert > 10) {
                cacheManager.dispatchList();
            }
            if (cacheManager.getIdSeq() == cacheManager.getTotal()) {
                /* 1⁰ Despachar últimos recebidos;
                *  2⁰ Reenviar restantes da cache.
                * */
                System.out.println("Produção finalizada, reenviar restantes.");

                // Reinicializar valor de total.
                cacheManager.setTotal(-1);
                cacheManager.setIdSeq(0);
                cacheManager.dispatchList();
                System.out.println("Reenviando " + cacheManager.cacheSize() + " itens não recebidos.");
                reSend();
            }
            stop = System.nanoTime();
            convert = TimeUnit.SECONDS.convert(stop - start, TimeUnit.NANOSECONDS);
            if (convert >  6)
                start = System.nanoTime();
        }
    }

    private void reSend() {
        // Reenviar todos os itens presentes na cache!
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
