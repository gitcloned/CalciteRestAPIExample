package org.gitcloned.nse;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import org.gitcloned.nse.http.NseHTTP;
import org.htmlunit.WebResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;

public class NseData {

    Logger logger = LoggerFactory.getLogger(NseData.class);

    private final NseHTTP requestBuilder;
    public String EQUITY_URL;

    public NseData(String EQUITY_URL, NseHTTP requestBuilder) {
        this.EQUITY_URL = EQUITY_URL;
        this.requestBuilder = requestBuilder;
    }

    public NseHTTP getRequestBuilder() {
        return requestBuilder;
    }

    public JsonArray scanTable (String groupName, String tableName) throws IOException, RuntimeException {
        WebResponse response = this.requestBuilder.sendGetRequest(EQUITY_URL + URLEncoder.encode(tableName, StandardCharsets.UTF_8.toString()));

        if (!NseHTTP.isSuccessfulResponse(response.getStatusCode())) {
            throw new RuntimeException("Data Request Failed, status code (" + response.getStatusCode() + "), Response: " + response.getStatusMessage());
        }

        String responseString = response.getContentAsString();

        JsonParser parser = new JsonParser();
        JsonElement tree = parser.parse(responseString);

        if (tree.isJsonObject()) {

            JsonElement rows = tree.getAsJsonObject().get("data");
            JsonElement time = tree.getAsJsonObject().get("timestamp");

            if (time.isJsonNull()) {
                throw new RuntimeException("Data Request Failed: Response is not a valid json, cannot find the 'time' of the data response");
            }

            String timeString = time.getAsString();

            logger.debug(String.format("fetched data as of time: '%s'", time));

            if (rows.isJsonArray()) {

                JsonArray data = rows.getAsJsonArray();

                for (JsonElement row : data) {

                    if (row.isJsonObject()) {

                        ((JsonObject)row).addProperty("time", timeString);
                    } else {
                        throw new RuntimeException("Data Request Failed: Response is not a valid json, each row in 'data' should be a valid json object");
                    }
                }

                return data;
            }
            else {
                throw new RuntimeException("Data Request Failed: Response is not a valid json, should have 'data' as array of json objects");
            }
        } else {
            throw new RuntimeException("Data Request Failed: Response is not a valid json, should be a json object");
        }
    }
}
