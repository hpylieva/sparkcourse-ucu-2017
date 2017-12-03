package football.configs;

import org.apache.spark.SparkConf;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;

@Configuration
@Profile("default")
public class DevConfig {
    @Bean
    public SparkConf sparkConf(){
        return new SparkConf().setMaster("local")
                .setAppName("football");
    }

    
}
