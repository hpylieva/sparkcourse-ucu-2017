package football.configs;

import lombok.Getter;
import lombok.SneakyThrows;
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
    private Map<String, List<String>> teams = new HashMap<>();

    @Getter
    private List<String> codesWithOneParticipant = Arrays.asList("6", "10");

    @Value("${columnNames}")
    private void setColumnNames(String[] columnNames) {
        this.columnNames = Arrays.asList(columnNames);
    }

    @PostConstruct
    public void initialize() {
        this.codes = readProperties("codes.properties");

        readProperties("teams.properties")
                .forEach((country, players) ->
                        teams.put(country, Arrays.asList(players.split(","))) //\s*,\s*
                );
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
