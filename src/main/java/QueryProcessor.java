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
    private static HashMap<Map.Entry<String, String>, Link>[] temp = null;
    private static String[] words = null;
    private static int parting = 0;
    private static int wordEnding = 0;
    private static MongoCollection<Document>[] myCollection = null;
    static int mini = 0;
    private static HashMap<String, org.jsoup.nodes.Document> myDocs = null;

    public static HashMap<Map.Entry<String, String>, Link> process(String query, MongoCollection<Document> collection,
            HashMap<String, org.jsoup.nodes.Document> docs,HashMap<String,Double>scores)
            throws InterruptedException {
        if(query.charAt(0)=='"')
            phraseSearch1(query,collection,docs,scores);

        myDocs = docs;
        query = RemoveStopWords.removeStopWords(query);
        words = query.split("\\s+");
        HashSet tempSet = new HashSet<>(Arrays.asList(words));
        words = (String[]) tempSet.toArray(new String[tempSet.size()]);
        mini = Math.min(words.length, Runtime.getRuntime().availableProcessors());
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
        System.out.println("Before Document");
        for (int index = 0; index < mini; index++) {
            th[index].join();
        }
        System.out.println("After Document");
        HashMap<Map.Entry<String, String>, Link> neededMap = new HashMap<>();
        for (int index = 0; index < mini; index++) {
            for (Map.Entry<Map.Entry<String, String>, Link> elem : temp[index].entrySet()) {
                if (neededMap.containsKey(elem.getKey())) {
                    Link newLink = elem.getValue();
                    newLink=neededMap.get(elem.getKey()).addLink(newLink);
                    neededMap.put(elem.getKey(),newLink);
                } else {
                    neededMap.put(elem.getKey(), elem.getValue());
                }
            }
        }
        Ranker r = new Ranker(words,neededMap,docs,scores);
        r.rank();
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
                        if (!temp[id].containsKey(Map.entry(link, words[index]))) {
                            linkObj = new Link();
                            linkObj.TF = arr.get(i).getInteger("TF");
                        } else {
                            linkObj = temp[id].get(Map.entry(link, words[index]));
                        }
                        switch (arr.get(i).getInteger("priority")) {
                            case 0:
                                linkObj.titleCount += arr.get(i).getInteger("count");
                                break;
                            case 1:
                                linkObj.highHeaderCount += arr.get(i).getInteger("count");
                                break;
                            case 2:
                                linkObj.descriptionCount += arr.get(i).getInteger("count");
                                break;
                            case 3:
                                linkObj.boldCount += arr.get(i).getInteger("count");
                                break;
                            case 4:
                                linkObj.pCount += arr.get(i).getInteger("count");
                                break;
                            default:
                                linkObj.lowHeaderCount += arr.get(i).getInteger("count");
                                break;
                        }
                        temp[id].put(Map.entry(link, words[index]), linkObj);
                    }
                }
            }
            for (Map.Entry<Map.Entry<String, String>, Link> elem : temp[id].entrySet()) {
                elem.getValue().URL = myDocs.get(elem.getKey().getKey());
                temp[id].put(elem.getKey(), elem.getValue());
            }
        }
    }
    public static HashMap<String, Integer> phraseSearch1(String query, MongoCollection<Document> collection, HashMap<String, org.jsoup.nodes.Document> docs,HashMap<String,Double>scores) throws InterruptedException {
        myDocs = docs;

        String query2 = query;
        String query1 = query;
        int o=0;
        String[] who = query.split("\\s+");
        int andorand = -1;
        int ind = -1;
        for (int i = 0; i < who.length; i++) {
            if (who[i].equals("and"))
            {
                andorand = 1;
                ind = i;
                break;
            }
            if (who[i].equals("or"))
            {
                ind = i;
                andorand = 2;
                break;
            }
            if (who[i].equals("not"))
            {
                ind = i;
                andorand = 3;
                break;
            }
            o+=who[i].length()+1;
        }
        if (andorand == -1) {
            return search(who, collection, docs);
        }
        if (andorand == 1) {
            query1 = query1.substring(0, o-1);
            query2 = query2.substring(o+4, query2.length());
            query1 = RemoveStopWords.removeStopWords(query1);
            query2 = RemoveStopWords.removeStopWords(query2);
            String[] wo1 = query1.split("\\s+");
            String[] wo2 = query2.split("\\s+");
            HashMap<String, Integer> res1 = search(wo1, collection, docs);
            HashMap<String, Integer> res2 = search(wo2, collection, docs);
            HashMap<String, Integer> result = new HashMap<>();
            for (Map.Entry<String, Integer> elem1 : res1.entrySet())
            {
                for (Map.Entry<String, Integer> elem2 : res2.entrySet())
                {
                    if(elem1.getKey().equals(elem2.getKey()))
                    {
                        result.put(elem1.getKey(),elem1.getValue());
                    }
                }
            }
            return result;
        }
        if (andorand == 2) {
            query1 = query1.substring(0, o-1);
            query2 = query2.substring(o+3, query2.length());
            query1 = RemoveStopWords.removeStopWords(query1);
            query2 = RemoveStopWords.removeStopWords(query2);
            String[] wo1 = query1.split("\\s+");
            String[] wo2 = query2.split("\\s+");
            HashMap<String, Integer> res = search(wo1, collection, docs);
            res.putAll(search(wo2, collection, docs));
            return res;
        }
        if (andorand == 3) {
            query1 = query1.substring(0, o-1);
            query2 = query2.substring(o+4, query2.length());
            query1 = RemoveStopWords.removeStopWords(query1);
            query2 = RemoveStopWords.removeStopWords(query2);
            String[] wo1 = query1.split("\\s+");
            String[] wo2 = query2.split("\\s+");
            HashMap<String, Integer> res1 = search(wo1, collection, docs);
            HashMap<String, Integer> res2 = search(wo2, collection, docs);
            HashMap<String, Integer> result = new HashMap<>();
            for (Map.Entry<String, Integer> elem1 : res1.entrySet())
            {
                boolean flag=false;
                for (Map.Entry<String, Integer> elem2 : res2.entrySet())
                {
                    if(elem1.getKey().equals(elem2.getKey()))
                    {
                        flag=true;
                        break;
                    }
                }
                if(flag==false)
                {
                    result.put(elem1.getKey(),elem1.getValue());
                }
            }
            return result;
        }
        return null;
    }

    public static HashMap<String, Integer> search(String[] words, MongoCollection<Document> collection, HashMap<String, org.jsoup.nodes.Document> docs) throws InterruptedException {
        myDocs = docs;
        HashMap<String, Integer> havelinks = new HashMap<>();
        ArrayList<Document> arr = null;
        for (int i = 0; i < words.length; i++) {
            Document docQuery = new Document("word",Indexer.porterStemmer.stemWord(words[i]));
            String link;
            for (Document document : collection.find(docQuery)) {
                arr = (ArrayList<Document>) document.get("values");
                HashSet<String> currLinks = new HashSet<>();
                for (int j = 0; j < arr.size(); j++) {
                    link = (String) arr.get(j).get("Link");
                    currLinks.add(link);
                }
                for (String elem : currLinks) {
                    if (havelinks.containsKey(elem)) {
                        havelinks.put(elem, havelinks.get(elem) + 1);
                    } else havelinks.put(elem, 1);
                }
            }
        }
        HashMap<String, Integer> Linkes = new HashMap<>();
        for (Map.Entry<String, Integer> elem : havelinks.entrySet()) {
            if (elem.getValue() != words.length)
                continue;
            int[] arr123 = new int[words.length];
            for (int i = 0; i < words.length; i++) {
                arr123[i] = -1;
            }
            boolean exit = false;
            boolean f = false;
            org.jsoup.nodes.Document doc = docs.get(elem.getKey());
            String text = doc.text();
            text = text.toUpperCase();
            String[] textsearch = text.split("\\s+");
            //System.out.println(words.length);
            for (int i = 0; i < words.length; i++)
            {
                //words[i] = Indexer.porterStemmer.stemWord(words[i]);
                int k;
                if (i == 0)
                    k = 0;
                else
                    k = arr123[i - 1];
                for (int j = k; j < textsearch.length; j++)
                {
                    if (i==0&&words[i].equals(textsearch[j]))
                    {
                        arr123[i] = j;
                        break;
                    }
                    if (words[i].equals(textsearch[j]) && i > 0 && arr123[i - 1] < j) {
                        arr123[i] = j;
                        break;
                    }
                    if (i > 0 && arr123[i - 1] > arr123[i] && arr123[i] != -1) {
                        exit = true;
                        break;
                    }

                }
                if (arr123[0] == -1) {
                    exit = true;
                }
                if (i > 0 && arr123[i - 1] == -1) {
                    exit = true;
                }
                if (i > 0 && arr123[i] == -1) {
                    exit = true;
                }
                if (exit == true) {
                    f = true;
                    break;
                }
            }
            if (f == false)
            {
                int sum = 0;
                for (int o = 0; o < words.length; o++)
                {
                    sum += arr123[o];
                }
                Linkes.put(elem.getKey(), sum);
            }
        }
        return Linkes;
    }

}