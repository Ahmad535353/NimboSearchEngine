import java.io.UnsupportedEncodingException;

public class LanguageDetector {
    public static Boolean IsEnglish(String str, double d)
    {
        try {
            if (str.length() == 0)
                return false;
            if((str.getBytes("UTF-8").length-str.length())/str.length()<d)
            {
                return true;
            }
            else return false;
        } catch (UnsupportedEncodingException e) {
            //e.printStackTrace();
        }
        return true;
    }
}
