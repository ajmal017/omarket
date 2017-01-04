package org.omarket;

import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.omarket.trading.ContractDB;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;

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
        JsonArray values = loadContracts(contractsDirPath, filter);
        assertEquals(98, values.size());
    }

    @Test
    public void loadOneContract() throws Exception {
        Path contractsDirPath = Paths.get(ClassLoader.getSystemResource("contracts").toURI());
        JsonObject contract = loadContract(contractsDirPath, "114584777");
        System.out.println(contract);
        JsonObject contractJson = contract.getJsonObject("m_contract");
        assertEquals(new Integer(114584777), contractJson.getInteger("m_conid"));
    }

    @Test(expected = IOException.class)
    public void loadOneContractFail() throws Exception {
        Path contractsDirPath = Paths.get(ClassLoader.getSystemResource("contracts").toURI());
        loadContract(contractsDirPath, "abcdef");
    }
}
