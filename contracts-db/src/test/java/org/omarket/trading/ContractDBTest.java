package org.omarket.trading;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;

import static org.junit.Assert.assertEquals;

/**
 * Created by Christophe on 04/01/2017.
 */
@RunWith(SpringRunner.class)
@SpringBootTest
public class ContractDBTest {

    @Autowired
    private ContractDBService service;

    @Test
    public void loadContractsAll() throws Exception {
        Path contractsDirPath = Paths.get(ClassLoader.getSystemResource("contracts").toURI());
        ContractFilter filter = ContractDBService.FILTER_NONE;
        rx.Observable<Security> contractsStream = service.loadContracts(contractsDirPath, filter);
        contractsStream.count().last().subscribe(x -> assertEquals(Integer.valueOf(98), x));
    }

    @Test
    public void loadContractsCurrencyExchange() throws Exception {
        ContractFilter filter = new ContractFilter() {
            @Override
            public boolean accept(String content) {
                return getPrimaryExchange().equals("ARCA") && getSecurityType().equals("STK")  && getCurrency().equals("USD");
            }
        };
        Path contractsDirPath = Paths.get(ClassLoader.getSystemResource("contracts").toURI());
        rx.Observable<Security> contractsStream = service.loadContracts(contractsDirPath, filter);
        contractsStream.count().last().subscribe(x -> assertEquals(Integer.valueOf(17), x));
    }

    @Test
    public void loadOneContract() throws Exception {
        Path contractsDirPath = Paths.get(ClassLoader.getSystemResource("contracts").toURI());
        Security contract = service.loadContract(contractsDirPath, "114584777");
        assertEquals("114584777", contract.getCode());
    }

    @Test(expected = IOException.class)
    public void loadOneContractFail() throws Exception {
        Path contractsDirPath = Paths.get(ClassLoader.getSystemResource("contracts").toURI());
        service.loadContract(contractsDirPath, "abcdef");
    }

    @Test
    public void loadContractsChainedFilters() throws Exception {
        ContractFilter currencyFilter = service.filterCurrency("USD");
        ContractFilter exchangeFilter = service.filterExchange("ARCA");
        ContractFilter typeFilter = service.filterSecurityType("STK");
        ContractFilter filter = service.composeFilter(currencyFilter, exchangeFilter,typeFilter);
        Path contractsDirPath = Paths.get(ClassLoader.getSystemResource("contracts").toURI());
        rx.Observable<Security> contractsStream = service.loadContracts(contractsDirPath, filter);
        contractsStream.count().last().subscribe(x -> assertEquals(Integer.valueOf(17), x));
        contractsStream.subscribe(details -> {
            assertEquals("USD", details.getCurrency());
            assertEquals("STK", details.getSecurityType());
            assertEquals("ARCA", details.getExchange());
        });
    }

    @Test
    public void loadMoreContractsChainedFilters() throws Exception {
        ContractFilter currencyFilter = service.filterCurrency("USD");
        ContractFilter typeFilter = service.filterSecurityType("STK");
        ContractFilter filter = service.composeFilter(currencyFilter, typeFilter);
        Path contractsDirPath = Paths.get(ClassLoader.getSystemResource("contracts").toURI());
        rx.Observable<Security> contractsStream = service.loadContracts(contractsDirPath, filter);
        contractsStream.count().last().subscribe(x -> assertEquals(Integer.valueOf(98), x));
        contractsStream.subscribe(details -> {
            assertEquals("USD", details.getCurrency());
            assertEquals("STK", details.getSecurityType());
        });
    }
}
