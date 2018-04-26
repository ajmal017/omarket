package org.omarket.contractinfo;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import com.google.gson.stream.JsonReader;
import lombok.extern.slf4j.Slf4j;
import org.omarket.quotes.Security;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.ComponentScan;

import java.io.IOException;
import java.lang.reflect.Type;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;

import static java.lang.String.format;

@Slf4j
@SpringBootApplication
public class ContractInfoApplication {

	public static void main(String[] args) {
		SpringApplication.run(ContractInfoApplication.class, args);
        ContractInfoApplication infoApp = new ContractInfoApplication();
        infoApp.run();
	}

    private void run() {
        try {
            URL etfsResource = Thread.currentThread().getContextClassLoader().getResource("etfs.json");
            if (etfsResource != null) {
                URI resourceURI = etfsResource.toURI();
                Path etfsPath = Paths.get(resourceURI);
                JsonReader reader = new JsonReader(Files.newBufferedReader(etfsPath));
                Type typeOfEtfsList = new TypeToken<List<String>>() {
                }.getType();
                Gson gson = new Gson();
                List<String> ibCodesETFs = gson.fromJson(reader, typeOfEtfsList);
                for(String ibCodeETF: ibCodesETFs){
                    log.info(format("sending: %s", ibCodeETF));
                }
            }
        } catch (URISyntaxException | IOException e) {
            log.error("failed to load resource: ", e);
        }
    }
}
