import java.util.ArrayList;
import java.util.HashMap;

import org.jsoup.nodes.Document;

import com.mongodb.client.MongoCollection;

import lib.PorterStemmer;
import lib.RemoveStopWords;

/**
 * QueryProcessor
 */
public class QueryProcessor {
    // the document and the count equation
    // count=count of specific priority * priority
    public static HashMap<String, Link> process(String query, MongoCollection<org.bson.Document> collection) {
        HashMap<String,Link>temp=new HashMap<>();
        query = RemoveStopWords.removeStopWords(query);
        String[] words = query.split("\\s+");
        PorterStemmer porterStemmer = new PorterStemmer();
        ArrayList<org.bson.Document> arr = null;
        String link;
        for (String word : words) {
            word=porterStemmer.stemWord(word);
            org.bson.Document docQuery = new org.bson.Document("word",word);
            for (org.bson.Document document : collection.find(docQuery)) {
                arr = (ArrayList<org.bson.Document>) document.get("values");
                for (int i = 0; i < arr.size(); i++) {
                    link=(String)arr.get(i).get("Link");
                    if (temp.containsKey(link)) {
                        
                    }
                }
            }
        }
        return null;
    }
}