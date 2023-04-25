import com.mongodb.*;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import org.bson.Document;

import java.io.IOException;
import java.util.concurrent.BrokenBarrierException;

public class Main {
    public static void main(String[] args) throws BrokenBarrierException, IOException, InterruptedException {
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
                MongoCollection myCollection = database.getCollection("Data");
                Indexer.index(myCollection);
            } catch (MongoException e) {
                e.printStackTrace();
            }
        }

    }
}
