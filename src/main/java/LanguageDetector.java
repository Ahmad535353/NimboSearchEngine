import org.apache.tika.language.LanguageIdentifier;

public class LanguageDetector {
    public String detectLanguage(){
        LanguageIdentifier identifier = new LanguageIdentifier("this is english ");
        String language = identifier.getLanguage();
        return language;
    }
}
