import java.net.MalformedURLException;
import java.net.URL;

public class myURL {
    private final String urlString;
    private final String domain;
    public myURL(String url) {
        this.urlString = url;
        URL temp = null;

        try {
            temp = new URL(urlString);
        } catch (MalformedURLException e) {
            e.printStackTrace();
        }
        this.domain = temp.getHost();

    }

    @Override
    public String toString() {
        return urlString;
    }

    public String getDomain() {
        return domain;
    }
}
