import com.mongodb.*;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;

import pojo.LinkDocument;

import java.io.EOFException;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BrokenBarrierException;
import org.bson.Document;
import org.jsoup.Jsoup;

public class Main {
    public static void main(String[] args) throws BrokenBarrierException, IOException, InterruptedException {
        HashMap<Map.Entry<String, String>, Link> testMap = null;
        String connectionString = "mongodb+srv://ahmedmohamed202:HacKeR2233@cluster0.lfmmw9l.mongodb.net/test";
        ServerApi serverApi = ServerApi.builder()
                .version(ServerApiVersion.V1)
                .build();
        MongoClientSettings settings = MongoClientSettings.builder()
                .applyConnectionString(new ConnectionString(connectionString))
                .serverApi(serverApi)
                .build();
        // Create a new client and connect to the server
        try (MongoClient mongoClient = MongoClients.create(settings)) {
            try {
                MongoDatabase database = mongoClient.getDatabase("Indexer");
                MongoCollection<Document> myCollection = database.getCollection("Data");
                // WebCrawler.crawl((short) 5);
                // HashMap<String, org.jsoup.nodes.Document> docs = WebCrawler.getDocs();
                // Indexer.index(myCollection);
                System.out.println("Connected...");
                HashMap<String, org.jsoup.nodes.Document> docs = new HashMap<>();
                List<LinkDocument> objectsList = new ArrayList<>();
                try {
                    ObjectInputStream objectInputStream = new ObjectInputStream(new FileInputStream("links.txt"));
                    while (true) {
                        try {
                            LinkDocument currLink = (LinkDocument) objectInputStream.readObject();
                            objectsList.add(currLink);
                        } catch (EOFException e) {
                            break;
                        }
                    }
                    objectInputStream.close();
                } catch (Exception e) {
                    e.printStackTrace();
                }

                for (int i = 0; i < objectsList.size(); i++) {
                    LinkDocument curr = (LinkDocument) objectsList.get(i);
                    docs.put(curr.link, Jsoup.parse(curr.document));
                }
                // Indexer.index(myCollection,docs);
                testMap = QueryProcessor.process("CSS in life and CSS in world", myCollection, docs);
                // System.out.println(testMap.size());
                System.out.println("Finished...");
            } catch (MongoException e) {
                e.printStackTrace();
            }
        }

    }
}
