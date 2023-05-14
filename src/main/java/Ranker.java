import org.jsoup.select.Elements;

import java.util.*;
import java.util.stream.Stream;

public class Ranker {
    HashMap<Map.Entry<String, String>, Link> links; //key->link , value->word
    HashMap<String, Double> scores;

    String[] words;
    private static final double k1 = 1.2;
    private static final double b = 0.75;
    private static final double k3 = 7.0;
    private HashMap<String, org.jsoup.nodes.Document> docs;
    static HashMap<String, Double> pagerank = new HashMap<>();


    public Ranker(String[] w, HashMap<Map.Entry<String, String>, Link> l, HashMap<String, org.jsoup.nodes.Document> doc, HashMap<String, Double> s) {
        links = l;
        words = w;
        docs = doc;
        scores = s;
    }

    public void rank() {
        HashMap<String, Double> idfMap = new HashMap<>();
        int numDocs = docs.size(); // total no. of docs

        for (String word : words) {
            int docsWithWord = 0;
            for (Map.Entry<String, String> temp : links.keySet()) {
                if (Objects.equals(temp.getValue(), word))
                    docsWithWord++;
            }
            System.out.println(docsWithWord);
            double idf = Math.log((numDocs - docsWithWord + 0.5) / (docsWithWord + 0.5));
            idfMap.put(word, idf);
        }
        for (Map.Entry<String, String> temp : links.keySet()) {
            int tf = links.get(temp).TF;
            double idf = idfMap.get(temp.getValue()); //link
            double words_num = docs.get(temp.getKey()).body().getAllElements().text().split("\\s+").length;
            double tags = (links.get(temp).pCount + links.get(temp).boldCount * 2 + links.get(temp).descriptionCount * 4 +
                    links.get(temp).highHeaderCount * 8 + links.get(temp).lowHeaderCount * 8 + links.get(temp).titleCount * 16) / words_num;
            if(!pagerank.containsKey(temp.getKey()))
                pagerank.put(temp.getKey(), 0.5*tf * idf + 0.2*tags);
            else {
                double x = pagerank.get(temp.getKey());
                pagerank.put(temp.getKey(), 0.5 * tf * idf + 0.3*tags + x+1);
            }
        }
        for(String link : pagerank.keySet())
        {
            double x = pagerank.get(link);
            pagerank.put(link,0.3*scores.get(link)+x);
        }
        System.out.println("Before sorting"+pagerank);
        reverseSortByValue(pagerank);
        System.out.println("After sorting"+pagerank);

    }
    public static void reverseSortByValue(HashMap<String, Double> hm) {
        // Create a list from elements of HashMap
        List<Map.Entry<String, Double>> list = new LinkedList<>(hm.entrySet());

        list.sort(new Comparator<Map.Entry<String, Double>>() {
            public int compare(Map.Entry<String, Double> o1, Map.Entry<String, Double> o2) {
                return o2.getValue().compareTo(o1.getValue());
            }
        });

        // Create a new LinkedHashMap to store the sorted entries
        LinkedHashMap<String, Double> sortedMap = new LinkedHashMap<>();
        for (Map.Entry<String, Double> entry : list) {
            sortedMap.put(entry.getKey(), entry.getValue());
        }
        pagerank=sortedMap;

    }
}

