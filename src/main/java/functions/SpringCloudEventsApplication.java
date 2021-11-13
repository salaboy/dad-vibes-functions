package functions;

import static org.springframework.cloud.function.cloudevent.CloudEventMessageUtils.ID;
import static org.springframework.cloud.function.cloudevent.CloudEventMessageUtils.SOURCE;
import static org.springframework.cloud.function.cloudevent.CloudEventMessageUtils.SPECVERSION;
import static org.springframework.cloud.function.cloudevent.CloudEventMessageUtils.SUBJECT;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;
import java.util.function.Function;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.cloud.function.cloudevent.CloudEventHeaderEnricher;
import org.springframework.cloud.function.web.util.HeaderUtils;
import org.springframework.context.annotation.Bean;
import org.springframework.http.HttpHeaders;
import org.springframework.messaging.Message;

@SpringBootApplication
public class SpringCloudEventsApplication {

  private static final Logger LOGGER = Logger.getLogger(
      SpringCloudEventsApplication.class.getName());

  public static void main(String[] args) {
    SpringApplication.run(SpringCloudEventsApplication.class, args);
  }

  private List<String> todos = new ArrayList<>();
  private List<String> meetingReminders = new ArrayList<>();
  
  @Bean
  public Function<Message<Input>, Output> addTODO(CloudEventHeaderEnricher enricher) {
    // If you are using CloudEvent Type based routing you should probably check that the CloudEvent that you 
    //  are getting is the one that you are expecting. 
    return m -> {
      HttpHeaders httpHeaders = HeaderUtils.fromMessage(m.getHeaders());

      log(httpHeaders);
      
      Input input = m.getPayload();
      LOGGER.log(Level.INFO, "Input {0} ", input);
      todos.add(input.input);
      Output output = new Output();
      output.input = input.input;
      output.operation = httpHeaders.getFirst(SUBJECT)+"-ACK";;
      output.output = input.input != null ? Arrays.toString(todos.toArray()) : "I don't remember anything to do.";
      return output;
    };
  }

  @Bean
  public Function<Message<Input>, Output> addMeetingReminder(CloudEventHeaderEnricher enricher) {
    // If you are using CloudEvent Type based routing you should probably check that the CloudEvent that you 
    //  are getting is the one that you are expecting. 
    return m -> {
      HttpHeaders httpHeaders = HeaderUtils.fromMessage(m.getHeaders());
      
      log(httpHeaders);

      Input input = m.getPayload();
      LOGGER.log(Level.INFO, "Input {0} ", input);
      meetingReminders.add(input.input);
      Output output = new Output();
      output.input = input.input;
      output.operation = httpHeaders.getFirst(SUBJECT)+"-ACK";
      output.output = input.input != null ? Arrays.toString(meetingReminders.toArray()) : "Weird.. I don't have any meetings to attend.";
      return output;
    };
  }

  @Bean
  public Function<Message<Input>, Output> dysfunction(CloudEventHeaderEnricher enricher) {
    // If you are using CloudEvent Type based routing you should probably check that the CloudEvent that you 
    //  are getting is the one that you are expecting. 
    return m -> {
      HttpHeaders httpHeaders = HeaderUtils.fromMessage(m.getHeaders());

      log(httpHeaders);

      Input input = m.getPayload();
      LOGGER.log(Level.INFO, "Input {0} ", input);
      todos.add(input.input);
      Output output = new Output();
      output.input = input.input;
      output.operation = httpHeaders.getFirst(SUBJECT)+"-ACK";;
      output.output = "Dysfunction is a function";
      return output;
    };
  }

  private void log(HttpHeaders httpHeaders) {
    LOGGER.log(Level.INFO, "Input CE Id:{0}", httpHeaders.getFirst(
      ID));
    LOGGER.log(Level.INFO, "Input CE Spec Version:{0}",
      httpHeaders.getFirst(SPECVERSION));
    LOGGER.log(Level.INFO, "Input CE Source:{0}",
      httpHeaders.getFirst(SOURCE));
    LOGGER.log(Level.INFO, "Input CE Subject:{0}",
      httpHeaders.getFirst(SUBJECT));
  }

  @Bean
  public CloudEventHeaderEnricher attributesProvider() {
    return attributes -> attributes
        .setSpecVersion("1.0")
        .setId(UUID.randomUUID()
            .toString())
        .setSource("http://salaboy.com/springnative-example")
        .setType("com.salaboy.springnative.events");
  }

  /**
   * Health checks
   * 
   * @return
   */
  @Bean
  public Function<String, String> health() {
    return probe -> {
      if ("readiness".equals(probe)) {
        return "ready";
      } else if ("liveness".equals(probe)) {
        return "live";
      } else {
        return "OK";
      }
    };
  }
}
