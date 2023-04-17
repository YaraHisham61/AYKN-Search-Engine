import org.jsoup.nodes.Document;

import java.util.HashMap;

public class Link {

    public void encode(Document doc, String url) {
        String titleTag = doc.select("title").text();
        //priority --> title - highHeader - description - b - span - p - lowHeaderTag
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

    }
}
