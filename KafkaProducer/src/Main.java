import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.StringTokenizer;
import java.util.stream.Collectors;

public class Main {

    public static void main(String[] args) throws IOException {

        // paths to files
        String devicesFile = "./../res/urzadzenia_rozliczeniowe_opis.csv";
        String recordsDir = "./../res/bialogard_archh_1";

        // kafka topic name
        String topic = "kafka-source";

        // producer configuration
        Properties props = new Properties();
        props.put("bootstrap.servers","localhost:9092");
        props.put("acks", "all");
        props.put("retries", 0);
        props.put("batch.size", 16384);
        props.put("linger.ms", 1);
        props.put("buffer.memory", 33554432);
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer","org.apache.kafka.connect.json.JsonSerializer");

        KafkaProducer<String, JsonNode> producer = new KafkaProducer<>(props);

        ObjectMapper objectMapper = new ObjectMapper();

        // send info about devices
        List<Device> devicesLines = new BufferedReader(new FileReader(devicesFile))
                .lines()
                .map(line -> new Device(
                        line.split(";")[0],
                        line.split(";")[1],
                        line.split(";")[2],
                        line.split(";")[4],
                        line.split(";")[9],
                        line.split(";")[14]
                        )
                )
                .collect(Collectors.toList());

        devicesLines.forEach(line -> {
            JsonNode jsonNode = objectMapper.valueToTree(line);
            producer.send(new ProducerRecord<String, JsonNode>(topic, jsonNode));
            //System.out.println(jsonNode);
        });

        producer.send(new ProducerRecord<String, JsonNode>(topic, objectMapper.valueToTree("EOF")));
        //System.out.println("EOF");


        // send records
        final File dir = new File(recordsDir);
        File[] listOfFiles = dir.listFiles();
        String[] listOfPaths = Arrays.stream(listOfFiles)
                .map(file -> file.getAbsolutePath())
                .toArray(String[]::new);
        Arrays.sort(listOfPaths);

        for(final String fileName : listOfPaths) {
            String deviceId = fileName.substring(fileName.length()-8, fileName.length()-4);

            List<Record> RecordsLines = new BufferedReader(new FileReader(fileName))
                    .lines()
                    .skip(3)
                    .filter(line -> !line.isEmpty())
                    .map(line -> {
                        StringTokenizer st = new StringTokenizer(line,";");
                        return new Record(
                                deviceId,
                                st.nextToken(),
                                st.nextToken(),
                                st.nextToken(),
                                st.nextToken(),
                                st.nextToken(),
                                st.nextToken(),
                                st.nextToken(),
                                st.nextToken(),
                                st.nextToken(),
                                st.nextToken());
                    })
                    .collect(Collectors.toList());

            RecordsLines.forEach(line -> {
                JsonNode jsonNode = objectMapper.valueToTree(line);
                producer.send(new ProducerRecord<String, JsonNode>(topic, jsonNode));
                //System.out.println(jsonNode);
            });

            producer.send(new ProducerRecord<String, JsonNode>(topic, objectMapper.valueToTree("EOF")));
            //System.out.println("EOF");
        }

        producer.close();
    }
}
