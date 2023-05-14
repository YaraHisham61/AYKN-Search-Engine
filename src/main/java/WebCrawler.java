import org.jsoup.Connection;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;

import pojo.LinkDocument;

import java.io.*;
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
    static int openedSize = 0;
    static Scanner scanner;
    static ArrayList<String> ls = new ArrayList<>();
    static int count = 0;

    static HashMap<String, Double> scores = new HashMap<>();
    static HashMap<String, HashMap<String, Integer>> pagerank = new HashMap<String, HashMap<String, Integer>>();

    static void paging(int iterations, double dampingfactor) throws IOException {
        for (int i = 1; i < iterations; i++) {
            for (String onePage : pagerank.keySet()) {
                double sum = 0;

                for (String parents : pagerank.get(onePage).keySet()) {
                    double ranko = scores.get(parents);
                    int grandparents = pagerank.get(parents).size();
                    if (grandparents == 0) sum += ranko; //seedLink it's hashMap size = 0
                    else sum += (ranko / grandparents);
                    /*if (sum == 0) {
                        System.out.println();
                    }*/

                }
                if (pagerank.get(onePage).size() != 0)
                    scores.put(onePage, sum);
            }
        }
        //System.out.println(scores);
        ObjectOutputStream outputStream = new ObjectOutputStream(new FileOutputStream("scores.txt"));
        for (String link : scores.keySet()) {
            PageRank tempLink = new PageRank(scores.get(link), link);
            outputStream.writeObject(tempLink);
        }
        outputStream.close();
    }

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
                if (Thread.interrupted()) {
                    arr[currNum - 1] = i;
                    arrEnd[currNum - 1] = end;
                }
                try {
                    theURL = ls.get(i);
                    con = Jsoup.connect(theURL);
                    doc = con.timeout(3000).userAgent("Mozilla/5.0").ignoreContentType(true).get();
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

                        if ((!temp.contains("https://")
                                && !temp.contains("http://"))) {
                            continue;
                        }
                        if (temp.contains(".com"))
                            addition.increment(temp, 0, theURL);
                        else if (temp.contains(".net"))
                            addition.increment(temp, 1, theURL);
                        else if (temp.contains(".edu"))
                            addition.increment(temp, 2, theURL);
                        else if (temp.contains(".gov"))
                            addition.increment(temp, 3, theURL);
                        else if (temp.contains(".org"))
                            addition.increment(temp, 4, theURL);
                        else
                            addition.increment(temp, 5, theURL);

                      //  docs.put(temp, doc);

                    }
                } catch (Exception e) {
                    continue;
                }
            }
        } catch (Exception e) {
        }
        boolean finished = false;

        try {
            cyclicBarrier.await();
        } catch (InterruptedException | BrokenBarrierException e) {
            throw new RuntimeException(e);
        }
        while (num < 6) {
            try {
                cyclicBarrier.await();
            } catch (InterruptedException | BrokenBarrierException e) {
                throw new RuntimeException(e);
            }
            for (Map.Entry<String, Integer> entry : splittedMaps[currNum - 1].entrySet()) {
                try {
                    temp = entry.getKey();
                    if (docs.size() >= 100) {
                        finished = true;
                        break;
                    }

                    if (temp.equals("https://www.pinterest.com/taste_of_home/") ||
                            temp.equals("https://www.pinterest.com/foxbroadcasting/"))
                        continue;
                    con = Jsoup.connect(temp);
                    doc = con.timeout(3000).userAgent("Mozilla/5.0").get();
                    if (con.response().statusCode() != 200) {
                        continue;
                    }

                    docs.put(temp, doc);
                    String Mylink = temp;

                    Elements links = doc.select("a[href]");
                    for (Element e : links) {
                        temp = e.attr("href");
                        if (temp.contains("/docs")
                                || temp.contains("/logs")
                                || (!temp.contains("https://")
                                && !temp.contains("http://"))) {
                            continue;
                        }
                        if (docs.size() >= 100) {
                            finished = true;
                            break;
                        }
                        addition.incrementLink(temp, num, Mylink);
                    }
                } catch (Exception ignore) {
                }
            }

            try {
                cyclicBarrier.await();
                System.out.println("out of barrier" + currNum);
            } catch (InterruptedException | BrokenBarrierException e) {
                throw new RuntimeException(e);
            }
            if (finished)
                break;
        }
    }

    static class Addition {
        public synchronized void increment(String s, int num, String ref) {
            if (m[num].containsKey(s))
                m[num].merge(s, 1, Integer::sum);
            else {
                size++;
                m[num].put(s, 1);
            }
//            if (!pagerank.containsKey(s)) {
//                HashMap<String, Integer> tempo = new HashMap<String, Integer>();
//                tempo.put(ref, 1);
//                pagerank.put(s, tempo);
//                //pagerank.get(s).put("yara", 1);
//            } else {
//                pagerank.get(s).put(ref, 1);
//            }
//            scores.put(s, 1.0);
//            if (!scores.containsKey(ref))
//                scores.put(ref, 1.0);////Ahmed ???????
//            if (!ls.contains(s))
//                ls.add(s);
        }

        public synchronized void incrementLink(String s, int num, String ref) throws IOException {
            if (!pagerank.containsKey(s)) {
                HashMap<String, Integer> tempo = new HashMap<String, Integer>();
                tempo.put(ref, 1);
                pagerank.put(s, tempo);
                //pagerank.get(s).put("yara", 1);
            } else {
                for (String y : pagerank.get(s).keySet())
                    System.out.println(y);

                System.out.println(pagerank.get(s).size());
                pagerank.get(s).put(ref, 1);
                System.out.println(pagerank.get(s).size());
            }
            scores.put(s, 1.0);
            if (!scores.containsKey(ref))
                scores.put(ref, 1.0);////Ahmed ???????
            if (!ls.contains(s))
                ls.add(s);
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
        List<Map.Entry<String, Integer>> list = new LinkedList<Map.Entry<String, Integer>>(hm.entrySet());

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
        System.setProperty("http.proxyhost", "127.0.0.1");// to hide ip data to avoid hackers
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
        for (String seedLink : ls) {
            pagerank.put(seedLink, new HashMap<String, Integer>());
            scores.put(seedLink, 4.0);
        }

        System.out.println("Reading Links Finished");
        count = ls.size();
        System.out.println("Size is  " + count);
        noOfThreads = args;
        splittedMaps = new HashMap[noOfThreads];

        for(int i = 0 ; i < noOfThreads;i++)
        {
            splittedMaps[i]=new HashMap<>();
        }
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
        // openedSize = m[0].size() + m[1].size() + m[2].size() + m[3].size() +
        // m[4].size() + m[5].size();
        System.out.println("Working on fetching");
//        for(int i = 0 ; i < noOfThreads;i++)
//        {
//            splittedMaps[i]=new HashMap<>();
//        }

        while (docs.size() < 100 && num < 6) {
            num++;
            // if (num == 0)
            // break;
            if (m[num].size() != 0) {
                splittedMaps = splitMap(m[num], noOfThreads);
            } else {
                num = -1;
                continue;
            }
            cyclicBarrier.await();
            System.out.println("Size is  " + size);
            cyclicBarrier.await();
        }
        System.out.println("Finished fetching");
        //cyclicBarrier.await();
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

        ObjectOutputStream outputStream = new ObjectOutputStream(new FileOutputStream("links.txt"));
        for (Map.Entry<String, Document> doc : docs.entrySet()) {
            LinkDocument tempLink = new LinkDocument();
            tempLink.link = doc.getKey();
            tempLink.document = doc.getValue().html();
            outputStream.writeObject(tempLink);
        }
        outputStream.close();
        // System.out.printf("Docs Size = %d", docs.size());
        // String URL;
        // Connection con;
        // Document doc;
        // for (int i = 0; i < 6; i++) {
        // Set<Map.Entry<String, Integer>> set = hm[i].entrySet();
        // for (Map.Entry<String, Integer> element : set) {
        // if (!docs.containsKey(element.getKey())) {
        // try {
        // URL = element.getKey();
        // con = Jsoup.connect(URL).timeout(3000).userAgent("Mozilla/5.0");
        // doc = con.get();
        // if (con.response().statusCode() != 200)
        // continue;
        // docs.put(URL, doc);
        // System.out.printf("The link is :- %s\nThe title is :- %s\n\n", URL,
        // doc.title());
        // } catch (Exception ignore) {
        // }
        //
        // } else {
        // System.out.printf("The link is :- %s\nThe title is :- %s\n\n",
        // element.getKey(), docs.get(element.getKey())) ;
        // }
        //
        // }
        // }
        paging(2, .85);

    }

    static Map<String, Integer>[] splitMap(Map<String, Integer> map, int numMaps) {
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