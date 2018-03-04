import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.ComponentScan;

@SpringBootApplication
@ComponentScan(basePackages = {"com.coco.kafka"})
@EnableAutoConfiguration
public class KafkaDemoApplication {

    public static void main(String[] args) throws Exception {
        SpringApplication.run(KafkaDemoApplication.class, args);
    }

}
