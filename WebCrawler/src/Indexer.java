import org.jsoup.nodes.Document;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import java.util.concurrent.BrokenBarrierException;

public class Indexer {

    public static void main(String[] args) throws IOException, BrokenBarrierException, InterruptedException {
        WebCrawler.crawl((short) 5);
        HashMap<String, Document> docs=WebCrawler.getDocs();
        System.out.println("The docs size is "+docs.size());
        Link link=new Link();
        for(Map.Entry<String,Document>doc: docs.entrySet())
        {
        link.encode(doc.getValue(), doc.getKey());
        }
    }
}
