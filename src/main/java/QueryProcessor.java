import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.Map;

import org.bson.Document;
import org.jsoup.Connection;
import org.jsoup.Jsoup;

import com.mongodb.client.MongoCollection;

import lib.PorterStemmer;
import lib.RemoveStopWords;

/**
 * QueryProcessor
 */
public class QueryProcessor {
    // the document and the count equation
    // count=count of specific priority * priority
    private static HashMap<String, Link>[] temp = null;
    private static String[] words = null;
    private static HashSet[] wordsSet = null;
    private static int parting = 0;
    private static int wordEnding = 0;
    private static MongoCollection<Document>[] myCollection = null;
    private static Object obj = new Object();
    static int mini = 0;

    public static HashMap<String, Link> process(String query, MongoCollection<Document> collection)
            throws InterruptedException {
        query = RemoveStopWords.removeStopWords(query);
        words = query.split("\\s+");
        HashSet tempSet = new HashSet<>(Arrays.asList(words));
        words =(String[]) tempSet.toArray(new String[tempSet.size()]);
        mini = Math.min(words.length, 5);
        if (words.length == 5) {
            parting = 1;
            wordEnding = 5;
        } else if (words.length > 5) {
            parting = words.length / 5;
            wordEnding = words.length;
        } else {
            parting = 1;
            wordEnding = words.length;
        }
        myCollection = new MongoCollection[mini];
        for (int i = 0; i < mini; i++) {
            myCollection[i] = collection;
        }
        ThreadedQuery tQuery = new ThreadedQuery();
        Thread[] th = new Thread[mini];
        for (int index = 0; index < mini; index++) {
            th[index] = new Thread(tQuery);
            th[index].setName(String.valueOf(index));
        }
        temp = new HashMap[mini];
        for (int index = 0; index < mini; index++) {
            temp[index] = new HashMap<>();
            th[index].start();
        }
        for (int index = 0; index < mini; index++) {
            th[index].join();
        }
        HashMap<String, Link> neededMap = new HashMap<>();
        for (int index = 0; index < mini; index++) {
            neededMap.putAll(temp[index]);
        }
        return neededMap;
    }

    static private class ThreadedQuery extends Thread {
        @Override
        public void run() {
            int id = Integer.parseInt(Thread.currentThread().getName());
            int start = id * parting;
            int end = (1 + id) * parting;
            if (mini > 5 && id == mini - 1)
                end = wordEnding;
            PorterStemmer porterStemmer = new PorterStemmer();
            ArrayList<Document> arr = null;
            String link;
            Link linkObj;
            for (int index = start; index < end; index++) {
                words[index] = porterStemmer.stemWord(words[index]);
                Document docQuery = new Document("word", words[index]);
                for (Document document : myCollection[id].find(docQuery)) {
                    arr = (ArrayList<Document>) document.get("values");
                    for (int i = 0; i < arr.size(); i++) {
                        link = (String) arr.get(i).get("Link");
                        if (!temp[id].containsKey(link)) {
                            linkObj = new Link();
                            linkObj.TF = arr.get(i).getInteger("TF");
                        } else {
                            linkObj = temp[id].get(link);
                        }
                        switch (arr.get(i).getInteger("priority")) {
                            case 0:
                                linkObj.titleCount = arr.get(i).getInteger("count");
                                break;
                            case 1:
                                linkObj.highHeaderCount = arr.get(i).getInteger("count");
                                break;
                            case 2:
                                linkObj.descriptionCount = arr.get(i).getInteger("count");
                                break;
                            case 3:
                                linkObj.boldCount = arr.get(i).getInteger("count");
                                break;
                            case 4:
                                linkObj.pCount = arr.get(i).getInteger("count");
                                break;
                            default:
                                linkObj.lowHeaderCount = arr.get(i).getInteger("count");
                                break;
                        }
                        synchronized (obj) {
                            temp[id].put(link, linkObj);
                        }
                    }
                }
            }
            for (Map.Entry<String, Link> elem : temp[id].entrySet()) {
                try {
                    Connection connection = Jsoup.connect(elem.getKey());
                    org.jsoup.nodes.Document doc = null;
                    // try again up to 5 turns
                    for (int index = 0; index < 3; index++) {
                        doc = connection.timeout(3000).get();
                        if (connection.response().statusCode() == 200)
                            break;
                    }
                    elem.getValue().URL = doc;
                    synchronized (obj) {
                        temp[id].put(elem.getKey(), elem.getValue());
                    }
                } catch (IOException ignore) {
                }
            }
        }
    }

    static Map<String, Link>[] splitMap(Map<String, Link> map, int numMaps) {
        int size = map.size();
        int chunkSize = (int) Math.ceil((double) size / numMaps);
        Map<String, Link>[] splitMaps = new Map[numMaps];

        int i = 0;
        int j = 0;
        for (String key : map.keySet()) {
            if (splitMaps[i] == null) {
                splitMaps[i] = new HashMap<>();
            }
            splitMaps[i].put(key, map.get(key));
            j++;
            if (j >= chunkSize && i < numMaps - 1) {
                i++;
                j = 0;
            }
        }

        return splitMaps;
    }

}