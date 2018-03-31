package org.omarket;

import org.junit.Test;
import org.omarket.trading.ContractDB;
import org.omarket.trading.Security;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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

    private static Logger logger = LoggerFactory.getLogger(ContractDBTest.class);

    @Test
    public void loadContractsAll() throws Exception {
        Path contractsDirPath = Paths.get(ClassLoader.getSystemResource("contracts").toURI());
        ContractDB.ContractFilter filter = ContractDB.FILTER_NONE;
        rx.Observable<Security> contractsStream = loadContracts(contractsDirPath, filter);
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
        rx.Observable<Security> contractsStream = loadContracts(contractsDirPath, filter);
        contractsStream.count().last().subscribe(x -> assertEquals(Integer.valueOf(17), x));
    }

    @Test
    public void loadOneContract() throws Exception {
        Path contractsDirPath = Paths.get(ClassLoader.getSystemResource("contracts").toURI());
        Security contract = loadContract(contractsDirPath, "114584777");
        assertEquals("114584777", contract.getCode());
    }

    @Test(expected = IOException.class)
    public void loadOneContractFail() throws Exception {
        Path contractsDirPath = Paths.get(ClassLoader.getSystemResource("contracts").toURI());
        loadContract(contractsDirPath, "abcdef");
    }

    @Test
    public void loadContractsChainedFilters() throws Exception {
        ContractDB.ContractFilter currencyFilter = ContractDB.filterCurrency("USD");
        ContractDB.ContractFilter exchangeFilter = ContractDB.filterExchange("ARCA");
        ContractDB.ContractFilter typeFilter = ContractDB.filterSecurityType("STK");
        ContractDB.ContractFilter filter = ContractDB.composeFilter(currencyFilter, exchangeFilter,typeFilter);
        Path contractsDirPath = Paths.get(ClassLoader.getSystemResource("contracts").toURI());
        rx.Observable<Security> contractsStream = loadContracts(contractsDirPath, filter);
        contractsStream.count().last().subscribe(x -> assertEquals(Integer.valueOf(17), x));
        contractsStream.subscribe(details -> {
            assertEquals("USD", details.getCurrency());
            assertEquals("STK", details.getSecurityType());
            assertEquals("ARCA", details.getExchange());
        });
    }

    @Test
    public void loadMoreContractsChainedFilters() throws Exception {
        ContractDB.ContractFilter currencyFilter = ContractDB.filterCurrency("USD");
        ContractDB.ContractFilter typeFilter = ContractDB.filterSecurityType("STK");
        ContractDB.ContractFilter filter = ContractDB.composeFilter(currencyFilter, typeFilter);
        Path contractsDirPath = Paths.get(ClassLoader.getSystemResource("contracts").toURI());
        rx.Observable<Security> contractsStream = loadContracts(contractsDirPath, filter);
        contractsStream.count().last().subscribe(x -> assertEquals(Integer.valueOf(98), x));
        contractsStream.subscribe(details -> {
            assertEquals("USD", details.getCurrency());
            assertEquals("STK", details.getSecurityType());
        });
    }
}
