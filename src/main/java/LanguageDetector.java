import org.apache.tika.language.LanguageIdentifier;

public class LanguageDetector {
    public String detectLanguage(String text){
        LanguageIdentifier identifier = new LanguageIdentifier(text);
        String language = identifier.getLanguage();
        return language;
    }
}
