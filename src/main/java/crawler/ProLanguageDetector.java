package crawler;

import com.google.common.base.Optional;
import com.optimaize.langdetect.LanguageDetectorBuilder;
import com.optimaize.langdetect.i18n.LdLocale;
import com.optimaize.langdetect.ngram.NgramExtractors;
import com.optimaize.langdetect.profiles.LanguageProfile;
import com.optimaize.langdetect.profiles.LanguageProfileReader;
import com.optimaize.langdetect.text.CommonTextObjectFactories;
import com.optimaize.langdetect.text.TextObject;
import com.optimaize.langdetect.text.TextObjectFactory;
import utils.MyEntry;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author amirphl
 */
public class ProLanguageDetector {

    private int numOfThreads;
    private HashMap<String, Integer> languages;
    private StringBuilder text;
    private String url;
    private TextObjectFactory[] textObjectFactories;
    private com.optimaize.langdetect.LanguageDetector[] languageDetectors;
    private final AtomicInteger atomicInteger = new AtomicInteger(0);
    private final FileWriter fw;
    private double accuracy;

    public ProLanguageDetector(int numOfThreads, double accuracy, File logpath) throws IOException {
        if (numOfThreads < 1)
            this.numOfThreads = 10; //default value
        this.numOfThreads = numOfThreads;

        if (accuracy > 1 || accuracy < 0)
            this.accuracy = 0.8;
        else
            this.accuracy = accuracy;

        //load all languages:
        List<LanguageProfile> languageProfiles = new LanguageProfileReader().readAllBuiltIn();
        languageDetectors = new com.optimaize.langdetect.LanguageDetector[numOfThreads];
        textObjectFactories = new TextObjectFactory[numOfThreads];
        //build language detector:
        for (int i = 0; i < numOfThreads; i++) {
            languageDetectors[i] = LanguageDetectorBuilder.create(NgramExtractors.standard())
                    .withProfiles(languageProfiles)
                    .build();
            //create a text object factory
            textObjectFactories[i] = CommonTextObjectFactories.forDetectingOnLargeText();
        }
        languages = new HashMap<>();
        fw = new FileWriter(logpath);
    }

    public void setvalues(String mUrl, StringBuilder mText) {
        text = mText;
        url = mUrl;
        try {
            fw.write(url + "\n" + numOfThreads + "\n");
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public boolean detectLanguage() {
        int len = text.length();
        for (int i = 0; i < numOfThreads; i++) {
            Thread th = new Thread(new Processor(text.substring(i * len / numOfThreads, Math.min(text.length(), (i + 1) * len / numOfThreads)), i));
            th.start();
        }

        long t = System.currentTimeMillis();
        while (System.currentTimeMillis() - t < 200) {
            MyEntry<String, Integer> maxEntry = new MyEntry<>();
            maxEntry.setKeyVal("!", 0);
            Iterator it = languages.entrySet().iterator();

            while (it.hasNext()) {
                MyEntry<String, Integer> pair = (MyEntry<String, Integer>) it.next();
                if (pair.getValue() > maxEntry.getValue()) {
                    maxEntry = new MyEntry();
                    maxEntry.setKeyVal(pair.getKey(), pair.getValue());
                }
            }
            if (maxEntry.getValue() / numOfThreads > accuracy) {
                try {
                    fw.flush();
                } catch (IOException e) {
                    e.printStackTrace();
                }
                if (maxEntry.getKey().equals("en"))
                    return true;
                else
                    return false;
            }
        }
        try {
            fw.flush();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return false;
    }

    private class Processor implements Runnable {

        private String t;
        private int id;

        public Processor(String t, int id) {
            this.t = t;
            this.id = id;
        }

        @Override
        public void run() {
            long time = System.currentTimeMillis();

            TextObject textObject = textObjectFactories[id].forText(t);
            Optional<LdLocale> lang = languageDetectors[id].detect(textObject);

            synchronized (atomicInteger) {
                String l = lang.get().getLanguage();
                System.out.println(lang);
                if (languages.containsKey(l))
                    languages.put(l, languages.get(l) + 1);
                else
                    languages.put(l, 1);

                try {
                    fw.write("Thread " + id + " operation ended in : " + (System.currentTimeMillis() - time) + " ms" + "\n");
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
    }
}
