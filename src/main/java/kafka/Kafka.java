package kafka;

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import config.ConfigProperties;
import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.config.CookieSpecs;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.utils.URIBuilder;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.json.JSONArray;
import org.json.JSONObject;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;

/** Class to connect to Apache Kafka server.
 *
 * @author Ariadna de Arriba
 */
public class Kafka
{
    private static Properties properties = ConfigProperties.readProperties();

    /** This method calls the filtered stream endpoint and streams Tweets from it.
     *
     * @param bearerToken Authorization token to connect to Twitter API.
     * @throws URISyntaxException {@link URISyntaxException during the http request.}
     * @throws IOException {@link IOException trying to get the content from the stream or to read the line.}
     */
    public static void connectStream(String bearerToken) throws URISyntaxException, IOException
    {
        HttpClient httpClient = HttpClients.custom().setDefaultRequestConfig(RequestConfig.custom().setCookieSpec(CookieSpecs.STANDARD).build()).build();

        URIBuilder uriBuilder = new URIBuilder(properties.getProperty("stream_url"));

        HttpGet httpGet = new HttpGet(uriBuilder.build());
        httpGet.setHeader("Authorization", String.format("Bearer %s", bearerToken));

        HttpResponse response = httpClient.execute(httpGet);
        HttpEntity entity = response.getEntity();
        int i = 0;
        if (null != entity)
        {
            BufferedReader reader = new BufferedReader(new InputStreamReader((entity.getContent())));
            String line = reader.readLine();
            System.out.println("Tweet " + i++ + ":");
            System.out.println(line);

            //Configure the Producer
            Properties configProperties = new Properties();
            configProperties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,"localhost:9092");
            configProperties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
            configProperties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);

            Producer producer = new KafkaProducer<String, String>(configProperties);

            while (line != null)
            {
                if (!line.isEmpty())
                {
                    System.out.println(line);
                    JsonObject json = JsonParser.parseString(line).getAsJsonObject();
                    JsonObject data = json.get("data").getAsJsonObject();
                    String text = data.get("text").toString();
                    String lang = data.get("lang").getAsString();

                    org.json.JSONObject jsonObject = new org.json.JSONObject();
                    jsonObject.put("text", text);
                    jsonObject.put("lang", lang);
                    String jsonTweet = jsonObject.toString();

                    ProducerRecord<String, String> rec = new ProducerRecord<>(properties.getProperty("kafka_topic"), jsonTweet);
                    producer.send(rec);
                }
                line = reader.readLine();
            }
        }

    }

    /** Helper method to setup rules before streaming data.
     *
     * @param bearerToken Authorization token to connect to Twitter API.
     * @param rules Rules for streaming data.
     * @throws URISyntaxException {@link URISyntaxException during the http request.}
     * @throws IOException {@link IOException trying to get the content from the request.}
     */
    public static void setupRules(String bearerToken, Map<String, String> rules) throws IOException, URISyntaxException
    {
        List<String> existingRules = getRules(bearerToken);
        if (existingRules.size() > 0)
        {
            deleteRules(bearerToken, existingRules);
        }
        createRules(bearerToken, rules);
    }

    /** Helper method to create rules for filtering.
     *
     * @param bearerToken Authorization token to connect to Twitter API.
     * @param rules Rules for streaming data.
     * @throws URISyntaxException {@link URISyntaxException during the http request.}
     * @throws IOException {@link IOException trying to get the content from the request.}
     */
    private static void createRules(String bearerToken, Map<String, String> rules) throws URISyntaxException, IOException
    {
        HttpClient httpClient = HttpClients.custom().setDefaultRequestConfig(RequestConfig.custom().setCookieSpec(CookieSpecs.STANDARD).build()).build();

        URIBuilder uriBuilder = new URIBuilder(properties.getProperty("rules_url"));

        HttpPost httpPost = new HttpPost(uriBuilder.build());
        httpPost.setHeader("Authorization", String.format("Bearer %s", bearerToken));
        httpPost.setHeader("content-type", "application/json");
        StringEntity body = new StringEntity(getFormattedString("{\"add\": [%s]}", rules));
        httpPost.setEntity(body);
        HttpResponse response = httpClient.execute(httpPost);
        HttpEntity entity = response.getEntity();
        if (null != entity)
        {
            System.out.println(EntityUtils.toString(entity, "UTF-8"));
        }
    }

    /** Helper method to get existing rules.
     *
     * @param bearerToken Authorization token to connect to Twitter API.
     * @throws URISyntaxException {@link URISyntaxException during the http request.}
     * @throws IOException {@link IOException trying to get the content from the request.}
     */
    private static List<String> getRules(String bearerToken) throws URISyntaxException, IOException
    {
        List<String> rules = new ArrayList<>();
        HttpClient httpClient = HttpClients.custom().setDefaultRequestConfig(RequestConfig.custom().setCookieSpec(CookieSpecs.STANDARD).build()).build();

        URIBuilder uriBuilder = new URIBuilder(properties.getProperty("rules_url"));

        HttpGet httpGet = new HttpGet(uriBuilder.build());
        httpGet.setHeader("Authorization", String.format("Bearer %s", bearerToken));
        httpGet.setHeader("content-type", "application/json");
        HttpResponse response = httpClient.execute(httpGet);
        HttpEntity entity = response.getEntity();
        if (null != entity)
        {
            org.json.JSONObject json = new org.json.JSONObject(EntityUtils.toString(entity, "UTF-8"));
            if (json.length() > 1)
            {
                JSONArray array = (JSONArray) json.get("data");
                for (int i = 0; i < array.length(); i++)
                {
                    org.json.JSONObject jsonObject = (JSONObject) array.get(i);
                    rules.add(jsonObject.getString("id"));
                }
            }
        }
        return rules;
    }

    /** Helper method to delete rules.
     *
     * @param bearerToken Authorization token to connect to Twitter API.
     * @param existingRules Existing rules for streaming data.
     * @throws URISyntaxException {@link URISyntaxException during the http request.}
     * @throws IOException {@link IOException trying to get the content from the request.}
     */
    private static void deleteRules(String bearerToken, List<String> existingRules) throws URISyntaxException, IOException
    {
        HttpClient httpClient = HttpClients.custom().setDefaultRequestConfig(RequestConfig.custom().setCookieSpec(CookieSpecs.STANDARD).build()).build();

        URIBuilder uriBuilder = new URIBuilder(properties.getProperty("rules_url"));

        HttpPost httpPost = new HttpPost(uriBuilder.build());
        httpPost.setHeader("Authorization", String.format("Bearer %s", bearerToken));
        httpPost.setHeader("content-type", "application/json");
        StringEntity body = new StringEntity(getFormattedString("{ \"delete\": { \"ids\": [%s]}}", existingRules));
        httpPost.setEntity(body);
        HttpResponse response = httpClient.execute(httpPost);
        HttpEntity entity = response.getEntity();
        if (null != entity)
        {
            System.out.println(EntityUtils.toString(entity, "UTF-8"));
        }
    }

    /** Get formatted string from a string pattern and ids.
     *
     * @param string String to format.
     * @param ids Selected ids.
     * @return Returns a formatted string.
     */
    private static String getFormattedString(String string, List<String> ids)
    {
        StringBuilder sb = new StringBuilder();
        if (ids.size() == 1)
        {
            return String.format(string, "\"" + ids.get(0) + "\"");
        } else
        {
            for (String id : ids)
            {
                sb.append("\"" + id + "\"" + ",");
            }
            String result = sb.toString();
            return String.format(string, result.substring(0, result.length() - 1));
        }
    }

    /** Get formatted string from a string pattern and rules.
     *
     * @param string String's format.
     * @param rules Rules to be formatted.
     * @return Returns a formatted string.
     */
    private static String getFormattedString(String string, Map<String, String> rules)
    {
        StringBuilder sb = new StringBuilder();
        if (rules.size() == 1)
        {
            String key = rules.keySet().iterator().next();
            return String.format(string, "{\"value\": \"" + key + "\", \"tag\": \"" + rules.get(key) + "\"}");
        } else
        {
            for (Map.Entry<String, String> entry : rules.entrySet())
            {
                String value = entry.getKey();
                String tag = entry.getValue();
                sb.append("{\"value\": \"" + value + "\", \"tag\": \"" + tag + "\"}" + ",");
            }
            String result = sb.toString();
            return String.format(string, result.substring(0, result.length() - 1));
        }
    }
}