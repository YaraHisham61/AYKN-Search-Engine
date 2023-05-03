import lib.PorterStemmer;
import lib.RemoveStopWords;

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
    static HashMap<String, Integer> tfMap = new HashMap<>();
    static String currentLink = null;
    static org.bson.Document doc = null;

    static boolean isSpam = false;

    public static void index(MongoCollection collection)
            throws IOException, BrokenBarrierException, InterruptedException {
        WebCrawler.crawl((short) 5);
        dataCollection = collection;

        HashMap<String, Document> docs = WebCrawler.getDocs();// link and its document
        System.out.println("The docs size is " + docs.size());
        int x = 1;
        for (Map.Entry<String, Document> doc : docs.entrySet()) {
            System.out.println("x = " + x++);
            currentLink = doc.getKey();
            encode(doc.getValue(), doc.getKey());
            checkTF();
            hMap.clear();
            if (!isSpam)
                addToDatabase();
            freqMap.clear();
        }

    }

    public static void encode(Document doc, String url) {
        String titleTag = doc.select("title").text();
        // priority --> title - highHeader - description - b - span - p - lowHeaderTag
        String pTag = doc.select("p").text() + " " + doc.select("code").text();
        String highHeaderTag = doc.select("h1").text() + " "
                + doc.select("h2").text() + " "
                + doc.select("h3").text();
        String lowHeaderTag = doc.select("h4").text() + " "
                + doc.select("h5").text() + " "
                + doc.select("h6").text();
        String descriptionTag = doc.select("meta[name=description]").attr("content");
        String boldTag = doc.select("b").text() + " " + doc.select("strong").text()
                + " " + doc.select("i").text() + " " + doc.select("em").text()
                + " " + doc.select("blockquote").text() + " " + doc.select("span").text();
        titleTag = RemoveStopWords.removeStopWords(titleTag);
        pTag = RemoveStopWords.removeStopWords(pTag);
        descriptionTag = RemoveStopWords.removeStopWords(descriptionTag);
        boldTag = RemoveStopWords.removeStopWords(boldTag);
        highHeaderTag = RemoveStopWords.removeStopWords(highHeaderTag);
        lowHeaderTag = RemoveStopWords.removeStopWords(lowHeaderTag);
        countWords(titleTag, 0);
        countWords(pTag, 4);
        countWords(descriptionTag, 2);
        countWords(boldTag, 3);
        countWords(highHeaderTag, 1);
        countWords(lowHeaderTag, 5);
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
            if (StringUtil.isNumeric(word) || containsSpecialWord(word))
                continue;
            word = porterStemmer.stemWord(word);
            if (freqMap.containsKey(word)) {
                freqMap.put(Map.entry(word, priority), freqMap.get(word) + 1);
            } else {
                freqMap.put(Map.entry(word, priority), 1);
            }
            if (hMap.containsKey(word))
                hMap.put(word, hMap.get(word) + 1);
            else
                hMap.put(word, 1);
        }
        totalSize += freqMap.size();
    }

    static void checkTF() {
        for (Map.Entry<String, Integer> elem : hMap.entrySet()) {
            if (100.0 * elem.getValue() / totalSize >= 50) {
                isSpam = true;
                return;
            } else {
                tfMap.put(elem.getKey(), (int) Math.round(100.0 * elem.getValue() / totalSize));
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
                        .append("count", entry.getValue() + 1)
                        .append("Link", currentLink)
                        .append("TF", tfMap.get(entry.getKey().getKey()));
                filter = Filters.eq("word", entry.getKey().getKey());
                update = Updates.addToSet("values", doc);
                if (dataCollection.find(query).first() != null) {
                    updateResult = dataCollection.updateOne(filter, update);
                    if (updateResult.getModifiedCount() != 0)
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
