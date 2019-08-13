package org.gitcloned.nse;

import com.google.api.client.http.HttpRequest;
import com.google.api.client.http.HttpResponse;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import org.gitcloned.nse.http.NseHTTP;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class NseData {

    Logger logger = LoggerFactory.getLogger(NseData.class);

    private final NseHTTP requestBuilder;

    public NseData(NseHTTP requestBuilder) {
        this.requestBuilder = requestBuilder;
    }

    public NseHTTP getRequestBuilder() {
        return requestBuilder;
    }

    private boolean isSuccessfulResponse (int statusCode) {

        if (statusCode - 200 < 0) return false;
        if (statusCode - 200 >= 100) return false;
        return true;
    }

    public JsonArray scanTable (String groupName, String tableName) throws IOException, RuntimeException {

        StringBuilder api = new StringBuilder("");
        api.append(tableName);
        api.append("StockWatch.json");

        HttpRequest request = this.requestBuilder.buildGetRequest(api.toString(), null);

        HttpResponse response = request.execute();

        if (!isSuccessfulResponse(response.getStatusCode())) {
            throw new RuntimeException("Data Request Failed, status code (" + response.getStatusCode() + "), Response: " + response.parseAsString());
        }

        String responseString = response.parseAsString();

        JsonParser parser = new JsonParser();
        JsonElement tree = parser.parse(responseString);

        if (tree.isJsonObject()) {

            JsonElement rows = tree.getAsJsonObject().get("data");
            JsonElement time = tree.getAsJsonObject().get("time");

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
