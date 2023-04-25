import lib.PorterStemmer;

import org.bson.conversions.Bson;
import org.bson.types.ObjectId;
import org.jsoup.internal.StringUtil;
import org.jsoup.nodes.Document;

import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.Updates;
import com.mongodb.client.result.UpdateResult;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.BrokenBarrierException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class Indexer {
    static PorterStemmer porterStemmer = new PorterStemmer();
    static MongoCollection dataCollection = null;
    static int totalSize = 0;
    static Map<Map.Entry<String, Integer>, Integer> freqMap = new HashMap<>();
    static HashMap<String, Integer> hMap = new HashMap<>();
    static HashMap<ObjectId, Boolean> flagMap = new HashMap<>();
    static String currentLink = null;
    static org.bson.Document doc = null;

    public static void index(MongoCollection collection)
            throws IOException, BrokenBarrierException, InterruptedException {
        WebCrawler.crawl((short) 5);
        dataCollection = collection;
        // dataCollection.insertOne(new org.bson.Document("Name", "Ahmed"));
        HashMap<String, Document> docs = WebCrawler.getDocs();// link and its document
        System.out.println("The docs size is " + docs.size());
        // Link link = new Link();
        for (Map.Entry<String, Document> doc : docs.entrySet()) {
            currentLink = doc.getKey();
            encode(doc.getValue(), doc.getKey());
            checkTF();
            addToDatabase();
            freqMap.clear();
        }

    }

    public static void encode(Document doc, String url) {
        String titleTag = doc.select("title").text();
        // priority --> title - highHeader - description - b - span - p - lowHeaderTag
        String pTag = doc.select("p").text();
        String highHeaderTag = doc.select("h1").text() + " "
                + doc.select("h2").text() + " "
                + doc.select("h3").text();
        String lowHeaderTag = doc.select("h4").text() + " "
                + doc.select("h5").text() + " "
                + doc.select("h6").text();
        String spanTag = doc.select("span").text();
        String descriptionTag = doc.select("meta[name=description]").attr("content");
        String boldTag = doc.select("b").text() + " " + doc.select("strong").text();
        titleTag = RemoveStopWords.removeStopWords(titleTag);
        pTag = RemoveStopWords.removeStopWords(pTag);
        descriptionTag = RemoveStopWords.removeStopWords(descriptionTag);
        boldTag = RemoveStopWords.removeStopWords(boldTag);
        highHeaderTag = RemoveStopWords.removeStopWords(highHeaderTag);
        lowHeaderTag = RemoveStopWords.removeStopWords(lowHeaderTag);
        spanTag = RemoveStopWords.removeStopWords(spanTag);
        countWords(titleTag, 0);
        countWords(pTag, 1);
        countWords(descriptionTag, 2);
        countWords(boldTag, 3);
        countWords(highHeaderTag, 4);
        countWords(lowHeaderTag, 5);
        countWords(spanTag, 6);
    }

    static boolean containsSpecialWord(String str) {
        Pattern pattern = Pattern.compile("[^a-zA-Z0-9]");
        Matcher matcher = pattern.matcher(str);
        boolean isStringContainsSpecialCharacter = matcher.find();
        if (isStringContainsSpecialCharacter)
            return true;
        else
            return false;
    }

    // link and count
    static void countWords(String str, int priority) {
        String[] words = str.split("\\s+");
        for (String word : words) {
            if (StringUtil.isNumeric(word)||containsSpecialWord(word))
                continue;
            word = porterStemmer.stemWord(word);
            if (freqMap.containsKey(word)) {
                freqMap.put(Map.entry(word, priority), freqMap.get(word) + 1);
                hMap.put(word, hMap.get(word) + 1);
            } else {
                freqMap.put(Map.entry(word, priority), 1);
                hMap.put(word, 1);
            }
        }
        totalSize += freqMap.size();
    }

    static void checkTF() {
        for (Map.Entry<String, Integer> elem : hMap.entrySet()) {
            if (100.0 * elem.getValue() / totalSize >= 60) {
                freqMap.remove(Map.entry(elem.getKey(), 0));
                freqMap.remove(Map.entry(elem.getKey(), 1));
                freqMap.remove(Map.entry(elem.getKey(), 2));
                freqMap.remove(Map.entry(elem.getKey(), 3));
                freqMap.remove(Map.entry(elem.getKey(), 4));
                freqMap.remove(Map.entry(elem.getKey(), 5));
                freqMap.remove(Map.entry(elem.getKey(), 6));
            }
        }
    }

    // word priority count
    static void addToDatabase() {
        Bson filter = null;
        Bson update = null;
        UpdateResult updateResult = null;
        for (Map.Entry<Map.Entry<String, Integer>, Integer> entry : freqMap.entrySet()) {
            try {
                org.bson.Document query = new org.bson.Document().append("word", entry.getKey().getKey());
                doc = new org.bson.Document("priority", entry.getKey().getValue())
                        .append("count", entry.getValue()+1)
                        .append("Link", currentLink);
                filter = Filters.eq("word",entry.getKey().getKey());
                update = Updates.addToSet("values",doc);
                if (dataCollection.find(query).first() != null) {
                    updateResult = dataCollection.updateOne(filter,update);
                    System.out.println("An item has updated");
                } else {
                    dataCollection.insertOne(
                            new org.bson.Document("word", entry.getKey().getKey())
                                    .append("values", Arrays.asList(doc)));

                    System.out.println("Added a new item");
                }
            } catch (Exception e) {

            }

        }
    }
}
