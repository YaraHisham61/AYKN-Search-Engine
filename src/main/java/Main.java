import com.mongodb.*;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import java.io.IOException;
import java.util.concurrent.BrokenBarrierException;
import org.bson.Document;

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
                MongoCollection<Document> myCollection = database.getCollection("Data");
                //Indexer.index(myCollection);
                QueryProcessor.process("C in life", myCollection);
            } catch (MongoException e) {
                e.printStackTrace();
            }
        }

    }
}
