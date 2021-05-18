import config.ConfigProperties;
import kafka.Kafka;

import java.io.IOException;
import java.net.URISyntaxException;
import java.util.*;

/** Class for running Twitter Monitor.
 *
 * @author Ariadna de Arriba
 */
public class TwitterMonitor
{
    private static Properties properties = ConfigProperties.readProperties();

    /** Main function to run twitter monitor.
     *
     * @param args Main function arguments
     * @throws IOException {@link IOException while trying to connect stream or setting up rules.}
     * @throws URISyntaxException {@link URISyntaxException while trying to connect stream or setting up rules.}
     */
    public static void main(String args[]) throws IOException, URISyntaxException
    {
        String bearerToken = properties.getProperty("bearer_token");

        if (null != bearerToken)
        {
            Kafka.setupRules(bearerToken);
            Kafka.connectStream(bearerToken);
        } else
        {
            System.out.println("There was a problem getting you bearer token. Please make sure you set the BEARER_TOKEN environment variable");
        }
    }
}
