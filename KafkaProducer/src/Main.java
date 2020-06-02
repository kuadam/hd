import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.StringTokenizer;
import java.util.stream.Collectors;

public class Main {

    public static void main(String[] args) throws IOException {

        // paths to files
        Path devicesFile = Paths.get("./../res/urzadzenia_rozliczeniowe_opis.csv");
        Path recordsDir = Paths.get("./../res/bialogard_archh_1");

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

        // send records
        final File dir = new File(recordsDir.toString());
        File[] listOfFiles = dir.listFiles();
        String[] listOfPaths = Arrays.stream(listOfFiles)
                .map(file -> file.getAbsolutePath())
                .toArray(String[]::new);
        Arrays.sort(listOfPaths);

        for(final String fileName : listOfPaths) {
            String deviceId = fileName.substring(fileName.length()-8, fileName.length()-4);
            String tableType1 = recordsDir.getFileName().toString();

            List<Record> RecordsLines = new BufferedReader(
                    new InputStreamReader(new FileInputStream(fileName), "windows-1250"))
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

            producer.send(new ProducerRecord<String, JsonNode>(topic, objectMapper.valueToTree(tableType1)));
            //System.out.println(tableType1);

            RecordsLines.forEach(line -> {
                JsonNode jsonNode = objectMapper.valueToTree(line);
                producer.send(new ProducerRecord<String, JsonNode>(topic, jsonNode));
                //System.out.println(jsonNode);
            });

            producer.send(new ProducerRecord<String, JsonNode>(topic, objectMapper.valueToTree("EOF")));
            //System.out.println("EOF");
        }

        // send info about devices
        String tableType2 = devicesFile.getFileName().toString();

        List<Device> devicesLines = new BufferedReader(
                new InputStreamReader(new FileInputStream(devicesFile.toString()), "windows-1250"))
                .lines()
                .map(line -> {
                    String[] splitted = line.split(";");

                    return new Device(
                            splitted[0],
                            splitted[1],
                            splitted[2],
                            splitted[4],
                            splitted[9],
                            splitted[14]);
                })
                .collect(Collectors.toList());

        producer.send(new ProducerRecord<String, JsonNode>(topic, objectMapper.valueToTree(tableType2)));
        //System.out.println(tableType2);

        devicesLines.forEach(line -> {
            JsonNode jsonNode = objectMapper.valueToTree(line);
            producer.send(new ProducerRecord<String, JsonNode>(topic, jsonNode));
            //System.out.println(jsonNode);
        });

        producer.send(new ProducerRecord<String, JsonNode>(topic, objectMapper.valueToTree("EOF")));
        //System.out.println("EOF");

        producer.close();
    }
}