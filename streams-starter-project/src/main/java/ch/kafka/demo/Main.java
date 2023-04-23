package ch.kafka.demo;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Produced;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.Properties;

// Press Shift twice to open the Search Everywhere dialog and type `show whitespaces`,
// then press Enter. You can now see whitespace characters in your code.
public class Main {
    private static final Logger log = LoggerFactory.getLogger(Main.class);

    private static Topology createTopology() {
        final var inputTopic = "word-count-input";
        final var outputTopic = "word-count-output";

        var builder = new StreamsBuilder();
        KStream<String, String> stream = builder.stream(inputTopic);

        KTable<String, Long> wordCount = stream.mapValues(v -> v.toLowerCase())
                .flatMapValues(v -> Arrays.asList(v.split(" ")))
                .selectKey((ignoredKey, word) -> word)
                .groupByKey()
                .count();

        wordCount.toStream().to(outputTopic, Produced.with(Serdes.String(), Serdes.Long()));
        return builder.build();
    }

    public static void main(String[] args) {
        var props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "stream-starter-app");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put("auto.offset.reset", "earliest");

        var streams = new KafkaStreams(createTopology(), props);
        log.info("Running count stream");
        streams.start();
        log.info(streams.toString());

        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
    }
}