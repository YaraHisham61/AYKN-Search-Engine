import org.jsoup.Connection;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;

import java.io.File;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CyclicBarrier;


public class WebCrawler extends Thread {
    Addition addition = new Addition();
    static CyclicBarrier cyclicBarrier;
    static int size = 0;
    static HashMap<String, Integer>[] m = new HashMap[6];

    public static HashMap<String, Document> getDocs() {
        return docs;
    }

    static HashMap<String, Document> docs;
    static File file = new File("SeedLinks.txt");
    static short num = -1;
    static Map<String, Integer>[] splittedMaps;
    static short noOfThreads;
    static int parting;
    static int[] arr;
    static int[] arrEnd;
    static Scanner scanner;
    static boolean finished = false;
    static ArrayList<String> ls = new ArrayList<>();
    static int count = 0;

    public WebCrawler() throws IOException {
    }

    public void run() {
        short currNum = Short.parseShort(getName());
        Document doc;
        Connection con;
        String temp;
        String theURL;
        int start = (currNum - 1) * parting;
        int end = 0;
        if (currNum != noOfThreads)
            end = currNum * parting;
        else
            end = count;
        try {
            for (int i = start; i < end; i++) {
                if (currentThread().isInterrupted()) {
                    arr[currNum - 1] = i;
                    arrEnd[currNum - 1] = end;
                }
                try {
                    theURL = ls.get(i);
                    con = Jsoup.connect(theURL);
                    doc = con.timeout(3000).ignoreContentType(true).get();
                    if (con.response().statusCode() != 200) {
                        continue;
                    }
                    if (doc.title() == "" || doc.title() == " ")
                        continue;
                    docs.put(theURL, doc);
                    Elements links = doc.select("a[href]");
                    System.out.printf("The link is :- %s\nThe title is :- %s\n\n", theURL, doc.title());
                    for (Element e : links) {
                        temp = e.attr("href");

                        if (temp.contains("/docs")
                                || temp.contains("/logs")
                                || (!temp.contains("https://")
                                && !temp.contains("http://"))) {
                            continue;
                        }
                        if (temp.contains(".com"))
                            addition.increment(temp, 0);
                        else if (temp.contains(".net"))
                            addition.increment(temp, 1);
                        else if (temp.contains(".edu"))
                            addition.increment(temp, 2);
                        else if (temp.contains(".gov"))
                            addition.increment(temp, 3);
                        else if (temp.contains(".org"))
                            addition.increment(temp, 4);
                        else addition.increment(temp, 5);
                    }
                } catch (Exception ignore) {
                }
            }
        } catch (Exception ignore) {
        }

        try {
            cyclicBarrier.await();
        } catch (InterruptedException | BrokenBarrierException e) {
            throw new RuntimeException(e);
        }
        if (num < 2)
            return;
        while (num < 6) {
            try {
                cyclicBarrier.await();
            } catch (InterruptedException | BrokenBarrierException e) {
                throw new RuntimeException(e);
            }
            for (Map.Entry<String, Integer> entry : splittedMaps[currNum - 1].entrySet()) {
                try {
                    temp = entry.getKey();
//
//                    if (finished || size >= 7000)
//                        break;
                    if (temp.equals("https://www.pinterest.com/taste_of_home/") ||
                            temp.equals("https://www.pinterest.com/foxbroadcasting/"))
                        continue;
                    con = Jsoup.connect(temp);
                    doc = con.timeout(3000).get();
                    if (con.response().statusCode() != 200) {
                        continue;
                    }

                    docs.put(temp, doc);
                    Elements links = doc.select("a[href]");
                    for (Element e : links) {
                        temp = e.attr("href");
                        if (temp.contains("/docs")
                                || temp.contains("/logs")
                                || (!temp.contains("https://")
                                && !temp.contains("http://"))) {
                            continue;
                        }
                        addition.incrementLink(temp, num);
                    }
                } catch (Exception ignore) {
                }
            }

            try {
                cyclicBarrier.await();
            } catch (InterruptedException | BrokenBarrierException e) {
                throw new RuntimeException(e);
            }
        }

    }

    static class Addition {
        public synchronized void increment(String s, int num) {
            if (m[num].containsKey(s))
                m[num].merge(s, 1, Integer::sum);
            else {
                size++;
                m[num].put(s, 1);
            }
        }

        public synchronized void incrementLink(String s, int num) throws IOException {
            if (m[num].containsKey(s))
                m[num].merge(s, 1, Integer::sum);
            else {
                size++;
                m[num].put(s, 1);
                Connection connection = Jsoup.connect(s);
                Document doc = connection.timeout(3000).get();
                if (connection.response().statusCode() != 200)
                    return;
                if (doc.title() == "" || doc.title() == " ")
                    return;
                docs.put(s, doc);

                System.out.printf("The link is :- %s\nThe title is :- %s\n\n", s, doc.title());
            }
        }
    }

    public static HashMap<String, Integer> sortByValue(HashMap<String, Integer> hm) {
        // Create a list from elements of HashMap
        List<Map.Entry<String, Integer>> list =
                new LinkedList<Map.Entry<String, Integer>>(hm.entrySet());

        // Sort the list
        Collections.sort(list, new Comparator<Map.Entry<String, Integer>>() {
            public int compare(Map.Entry<String, Integer> o1,
                               Map.Entry<String, Integer> o2) {
                return (o1.getValue()).compareTo(o2.getValue());
            }
        });

        // put data from sorted list to hashmap
        HashMap<String, Integer> temp = new LinkedHashMap<String, Integer>();
        for (Map.Entry<String, Integer> aa : list) {
            temp.put(aa.getKey(), aa.getValue());
        }
        return temp;
    }

    public static void crawl(short args) throws IOException, InterruptedException, BrokenBarrierException {
        System.setProperty("http.proxyhost", "127.0.0.1");//to hide ip data to avoid hackers
        System.setProperty("http.proxyport", "3128");
        System.out.println("Starting Web Crawling");
        docs = new HashMap<>();
        scanner = new Scanner(file);
        m[0] = new HashMap<>();
        m[1] = new HashMap<>();
        m[2] = new HashMap<>();
        m[3] = new HashMap<>();
        m[4] = new HashMap<>();
        m[5] = new HashMap<>();
        while (scanner.hasNextLine()) {
            ls.add(scanner.nextLine());
        }
        System.out.println("Reading Links Finished");
        count = ls.size();
        System.out.println("Size is  " + count);
        noOfThreads = args;
        if (noOfThreads == 0 || noOfThreads > count)
            noOfThreads = (short) count;
        arr = new int[noOfThreads];
        arrEnd = new int[noOfThreads];
        cyclicBarrier = new CyclicBarrier(noOfThreads + 1);
        parting = count / noOfThreads;
        WebCrawler[] th = new WebCrawler[noOfThreads];
        for (int i = 0; i < noOfThreads; i++) {
            th[i] = new WebCrawler();
            th[i].setName(String.valueOf(i + 1));
        }
        for (int i = 0; i < noOfThreads; i++) {
            th[i].start();
        }
        cyclicBarrier.await();
        System.out.println("Working on fetching");
        while (size < 6000 && num < 6) {
            num++;
            if(num==0)
                break;
            splittedMaps = splitMap(m[num], noOfThreads);
            cyclicBarrier.await();
            System.out.println("Size is  " + size);
            cyclicBarrier.await();
        }

        for (int i = 0; i < noOfThreads; i++) {
            th[i].join();
        }
        System.out.println("Size is  " + size);
        Map<String, Integer> hm[] = new HashMap[6];
        System.out.println("Sorting Started");
        for (int i = 0; i < 6; i++) {
            hm[i] = sortByValue(m[i]);
        }
        int count = hm[0].size() + hm[1].size() + hm[2].size() +
                hm[3].size() + hm[4].size() + hm[5].size();
        System.out.println("Sorting Finished");
        System.out.println("Crawling finished");
//        System.out.printf("Docs Size = %d", docs.size());
//        String URL;
//        Connection con;
//        Document doc;
//        for (int i = 0; i < 6; i++) {
//            Set<Map.Entry<String, Integer>> set = hm[i].entrySet();
//            for (Map.Entry<String, Integer> element : set) {
//                if (!docs.containsKey(element.getKey())) {
//                    try {
//                        URL = element.getKey();
//                        con = Jsoup.connect(URL).timeout(3000);
//                        doc = con.get();
//                        if (con.response().statusCode() != 200)
//                            continue;
//                        docs.put(URL, doc);
//                        System.out.printf("The link is :- %s\nThe title is :- %s\n\n", URL, doc.title());
//                    } catch (Exception ignore) {
//                    }
//
//                } else {
//                    System.out.printf("The link is :- %s\nThe title is :- %s\n\n", element.getKey(), docs.get(element.getKey())) ;
//                }
//
//            }
//        }

    }

    private static Map<String, Integer>[] splitMap(Map<String, Integer> map, int numMaps) {
        int size = map.size();
        int chunkSize = (int) Math.ceil((double) size / numMaps);
        Map<String, Integer>[] splitMaps = new Map[numMaps];

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