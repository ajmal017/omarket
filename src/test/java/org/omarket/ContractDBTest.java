package org.omarket;

import io.vertx.core.json.JsonObject;
import org.junit.Test;
import org.omarket.trading.ContractDB;
import rx.Observable;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.omarket.trading.ContractDB.loadContract;
import static org.omarket.trading.ContractDB.loadContracts;

/**
 * Created by Christophe on 04/01/2017.
 */
public class ContractDBTest {

    @Test
    public void loadContractsAll() throws Exception {
        Path contractsDirPath = Paths.get(ClassLoader.getSystemResource("contracts").toURI());
        ContractDB.ContractFilter filter = ContractDB.ALL;
        rx.Observable<JsonObject> contractsStream = loadContracts(contractsDirPath, filter);
        contractsStream.count().last().subscribe(x -> assertEquals(Integer.valueOf(98), x));
    }

    @Test
    public void loadContractsCurrencyExchange() throws Exception {
        ContractDB.ContractFilter filter = new ContractDB.ContractFilter() {
            @Override
            public boolean accept(String content) {
                return getPrimaryExchange().equals("ARCA") && getSecurityType().equals("STK")  && getCurrency().equals("USD");
            }
        };
        Path contractsDirPath = Paths.get(ClassLoader.getSystemResource("contracts").toURI());
        rx.Observable<JsonObject> contractsStream = loadContracts(contractsDirPath, filter);
        contractsStream.count().last().subscribe(x -> assertEquals(Integer.valueOf(17), x));
    }

    @Test
    public void loadOneContract() throws Exception {
        Path contractsDirPath = Paths.get(ClassLoader.getSystemResource("contracts").toURI());
        JsonObject contract = loadContract(contractsDirPath, "114584777");
        JsonObject contractJson = contract.getJsonObject("m_contract");
        assertEquals(new Integer(114584777), contractJson.getInteger("m_conid"));
    }

    @Test(expected = IOException.class)
    public void loadOneContractFail() throws Exception {
        Path contractsDirPath = Paths.get(ClassLoader.getSystemResource("contracts").toURI());
        loadContract(contractsDirPath, "abcdef");
    }
}
