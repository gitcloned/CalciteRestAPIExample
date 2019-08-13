package org.gitcloned.nse.http;

import com.google.api.client.http.*;
import com.google.api.client.http.javanet.NetHttpTransport;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.security.GeneralSecurityException;
import java.util.Map;

public class NseHTTP {

    Logger logger = LoggerFactory.getLogger(NseHTTP.class);

    private final String BASE_URL;
    private HttpRequestFactory requestFactory;

    public NseHTTP(String BASE_URL) {

        this.BASE_URL = BASE_URL;
        this.requestFactory = new NetHttpTransport().createRequestFactory();

        try {

            NetHttpTransport.Builder builder = new NetHttpTransport.Builder();
            builder.doNotValidateCertificate();

            this.requestFactory = builder.build().createRequestFactory();
        } catch (GeneralSecurityException e) {
            throw new RuntimeException("Got General Security exception: " + e.getMessage());
        }
    }

    private void setHeaders (HttpRequest request, Map<String, String> headers) {
        if (headers != null) {

            HttpHeaders httpHeaders = request.getHeaders();

            for (String key : headers.keySet()) {
                httpHeaders.set(key, headers.get(key));
            }
        }
    }

    private void setAuth (HttpRequest request) {

        // No auth required
    }

    public HttpRequest buildGetRequest (String api, Map<String, String> headers) throws IOException {

        logger.info(String.format("Hitting api '%s'", BASE_URL + api));

        HttpRequest request = requestFactory.buildGetRequest(
                new GenericUrl(BASE_URL + api));

        setHeaders(request, headers);

        setAuth(request);

        return request;
    }
}
