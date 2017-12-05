package football.configs;

import lombok.Getter;
import lombok.SneakyThrows;
import org.apache.commons.collections.BidiMap;
import org.apache.commons.collections.bidimap.DualHashBidiMap;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import java.io.InputStream;
import java.io.Serializable;
import java.util.*;

@Component
public class UserConfig implements Serializable {
    @Getter
    private List<String> columnNames;

    @Getter
    private Map<String, String> codes;

    @Getter
    private Map<String,String> teams = new HashMap<>();

    @Getter
    private List<String> codesWithOneParticipant = Arrays.asList("6", "10");

    @Value("${columnNames}")
    private void setColumnNames(String[] columnNames) {
        // to get column names from property files
        this.columnNames = Arrays.asList(columnNames);
    }

    @PostConstruct
    public void initialize() {
        this.codes = readProperties("codes.properties");

        Map<String, String> teamsProp = readProperties("teams.properties");
        for(Object key : teamsProp.keySet()){
            String[] players =  teamsProp.get(key).split(",");
            for (String player: players) {
                teams.put( player,(String) key);
            }
        }
            //    .forEach((country, players) ->
             //           teams.put(country, Arrays.asList(players.split(","))));

    }

    @SneakyThrows
    private Map<String, String> readProperties(String propertiesFileName) {
        Properties properties = new Properties();
        properties.load(Thread.currentThread().getContextClassLoader().getResourceAsStream(propertiesFileName));

        HashMap<String, String> propertiesMap = new HashMap<>();
        Enumeration e = properties.propertyNames();

        while (e.hasMoreElements()) {
            String key = (String) e.nextElement();
            propertiesMap.put(key, properties.getProperty(key));
        }

        return propertiesMap;
    }

}
