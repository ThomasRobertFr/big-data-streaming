import java.util.*;
import java.io.*;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

import twitter4j.*;
import twitter4j.conf.ConfigurationBuilder;

import org.json.JSONObject;

public class ProducerKafka {

    public static void startProducerFromFile(String topic){
        File[] listOfFiles = getListFiles();
        Producer<String,String> producer = getProducer();
        for (int i = 0; i < listOfFiles.length; i++) {
            if (listOfFiles[i].isFile()) {
                sendMessageFromFile(producer, listOfFiles[i], topic);
            }
        }
    }

    public static File[] getListFiles(){
        File folder = new File("../datas");
        File[] listOfFiles = folder.listFiles();
        return listOfFiles;
    }

    public static Producer<String,String> getProducer(){
        Properties props=new Properties();
        props.put("metadata.broker.list", "casi-m.c.casi-bigdata.internal:9092,casi-w-0.c.casi-bigdata.internal:9093,casi-w-1.c.casi-bigdata.internal:9094");
        props.put("zk.connect","localhost:2181");
        props.put("serializer.class","kafka.serializer.StringEncoder");
        ProducerConfig producerConfig=new ProducerConfig(props);
        Producer<String,String> kafkaProducer=new Producer<String,String>(producerConfig);
        return kafkaProducer;
    }

    public static void sendMessageFromFile(Producer<String,String> producer, File file, String topic){
        try{
            BufferedReader reader=new BufferedReader(new InputStreamReader(new FileInputStream(file)));
            List<KeyedMessage<String,String>> msgList=new ArrayList<KeyedMessage<String,String>>();
            int batchSize=200;
            int count=0;
            while (true) {
                String line=reader.readLine();
                if (line == null){
                    break;
                }
                count++;
                JSONObject jsonObj = new JSONObject(line);
                String language = jsonObj.getString("lang");
                if(language.equals("en")){
                    String texte = jsonObj.getString("text");
                    KeyedMessage<String,String> msg=new KeyedMessage<String,String>(topic, texte);
                    msgList.add(msg);
                    if (msgList.size() > batchSize) {
                        producer.send(msgList);
                        msgList.clear();
                    }
                }
            }
            if (msgList.size() > 0) {
            producer.send(msgList);
            }
        }
        catch(Exception e) {
            System.out.println(e.getMessage());
        }
    }

    public static void startProducerFromTwitter(String topic, String[] word, String[] lang){
        Producer<String,String> producer = getProducer();
        ConfigurationBuilder cb = getConfigurationTwitter();
        StatusListener listener = getStatusListener(topic, producer);
        final TwitterStream twitterStream = new TwitterStreamFactory(cb.build()).getInstance();
        twitterStream.addListener(listener);
        FilterQuery filter = createFilter(word, lang);
        twitterStream.filter(filter);
    }

    public static ConfigurationBuilder getConfigurationTwitter(){
        ConfigurationBuilder cb = new ConfigurationBuilder();
        cb.setOAuthConsumerKey("setOAuthConsumerKey");
        cb.setOAuthConsumerSecret("setOAuthConsumerSecret");
        cb.setOAuthAccessToken("setOAuthAccessToken");
        cb.setOAuthAccessTokenSecret("setOAuthAccessTokenSecret");
        cb.setJSONStoreEnabled(true);
        cb.setIncludeEntitiesEnabled(true);
        return cb;
    }

    public static StatusListener getStatusListener(final String topic, final Producer<String,String> producer){
        StatusListener listener = new StatusListener() {

            public void onStatus(Status status) {
                final KeyedMessage<String, String> data = new KeyedMessage<String, String>(topic, status.getText());
                producer.send(data);
            }
            public void onDeletionNotice(StatusDeletionNotice statusDeletionNotice) {}

            public void onTrackLimitationNotice(int numberOfLimitedStatuses) {}

            public void onScrubGeo(long userId, long upToStatusId) {}

            public void onException(Exception ex) {
                System.out.println("Twitter exception :");
                System.out.println(ex.getMessage());
            }

            public void onStallWarning(StallWarning warning) {}
        };
        return listener;
    }

    public static FilterQuery createFilter(String[] wordToFilter, String[] lang){
        FilterQuery filter = new FilterQuery();
        filter.track(wordToFilter);
        filter.language(lang);
        return filter;
    }

    public static void main(String[] args) {
        try {
            System.out.println("Press 1 for fill the topic from Twitter Stream or 2 for fill the topic from Files");
            BufferedReader entree = new BufferedReader(new InputStreamReader(System.in));
            String inChar = entree.readLine();
            String topic = "TweetStatus";
            String[] word = {"#IronMan3"};
            String[] lang = {"en"};
            if(inChar.equals("1")){
                startProducerFromTwitter(topic, word, lang);
            }
            else if(inChar.equals("2")){
                startProducerFromFile(topic);
            }
        } catch (Exception e) {
            System.out.println(e.getMessage());
        }
    }

}
