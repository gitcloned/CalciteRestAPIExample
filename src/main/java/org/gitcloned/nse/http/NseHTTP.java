package org.gitcloned.nse.http;

import org.apache.commons.lang3.StringUtils;
import org.htmlunit.BrowserVersion;
import org.htmlunit.WebClient;
import org.htmlunit.WebResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Map;

public class NseHTTP {

    Logger logger = LoggerFactory.getLogger(NseHTTP.class);

    private WebClient webClient;

    public NseHTTP(String BASE_URL_FOR_COOKIES, String target) {
        this.webClient = new WebClient(BrowserVersion.CHROME);
        webClient.getOptions().setJavaScriptEnabled(false);
        webClient.getOptions().setCssEnabled(false);
        if(StringUtils.isNotBlank(BASE_URL_FOR_COOKIES)) {
            logger.info(String.format("Initializing web client cookies from %s for %s", BASE_URL_FOR_COOKIES, target));
            try {
                WebResponse response = webClient.getPage(BASE_URL_FOR_COOKIES).getWebResponse();
                if(200 != response.getStatusCode()) {
                    throw new RuntimeException(String.format("Can not initialize cookies using %s with result %s, code %s", BASE_URL_FOR_COOKIES, response.getStatusMessage(), response.getStatusCode()));
                }
            }
            catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
    }
    public static boolean isSuccessfulResponse (int statusCode) {
        if (statusCode - 200 < 0) return false;
        if (statusCode - 200 >= 100) return false;
        return true;
    }
    public WebResponse sendGetRequest (String api) throws IOException {
        logger.info(String.format("Hitting api '%s'", api));
        return webClient.getPage(api).getWebResponse();
    }
}
