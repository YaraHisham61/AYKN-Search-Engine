import java.io.Serializable;
import java.util.HashMap;

public class PageRank implements Serializable {
    Double score;
    String link;
    public PageRank(Double s,String l)
    {
        score=s;
        link=l;
    }

}
