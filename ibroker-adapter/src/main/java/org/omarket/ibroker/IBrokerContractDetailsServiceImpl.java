package org.omarket.ibroker;

import com.ib.client.Contract;
import com.ib.client.ContractDetails;
import lombok.extern.slf4j.Slf4j;
import org.omarket.quotes.Security;
import org.springframework.stereotype.Service;

import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;

import static java.lang.String.format;
import static java.lang.Thread.sleep;

/**
 * Created by Christophe on 03/11/2016.
 */
@Slf4j
@Service
public class IBrokerContractDetailsServiceImpl extends AbstractIBrokerClient implements IBrokerContractDetailsService {

    public static final int IB_MAX_SIMULTANEOUS_CONTRACT_DETAILS_REQUESTS = 10;
    public static final int PERIOD_CONTRACT_DETAILS_REQUEST_RETRY_MS = 1000;

    private Set<Integer> requestedContracts = Collections.synchronizedSet(new TreeSet<>());
    private Map<Integer, Set<Security>> resultsContractDetails = Collections.synchronizedMap(new TreeMap<>());

    private static Security fromContractDetails(ContractDetails contractDetails) {
        Contract contract = contractDetails.contract();
        String code = String.valueOf(contractDetails.conid());
        String symbol = contract.localSymbol();
        String currency = contract.currency();
        String primaryExchange = contract.primaryExch();
        String exchange = contract.exchange();
        String securityType = contract.getSecType();
        Double minTick = contractDetails.minTick();
        String priceMagnifier = String.valueOf(contractDetails.priceMagnifier());
        String longName = contractDetails.longName();
        return new Security(code, symbol, currency, primaryExchange, exchange, securityType, minTick, priceMagnifier, longName);
    }

    public IBrokerContractDetailsServiceImpl(){
    }

    /**
     *
     * @param contract
     * @return
     */
    @Override
    public void requestContractDetails(Contract contract) {
        Integer newRequestId = newRequestId();
        while(this.requestedContracts.size() >= IB_MAX_SIMULTANEOUS_CONTRACT_DETAILS_REQUESTS){
            try {
                sleep(PERIOD_CONTRACT_DETAILS_REQUEST_RETRY_MS);
            } catch (InterruptedException e) {
                log.error("interrupted request for contract details", e);
            }
        }
        resultsContractDetails.put(newRequestId, new TreeSet<>());
        log.info(format("request contract details: id %s", newRequestId));
        getClientSocket().reqContractDetails(newRequestId, contract);
    }

    @Override
    public void connect(Integer clientId, String ibrokesHost, Integer ibrokerPort) {

    }

    /**
     *
     * @param requestId
     * @param contractDetails
     */
    @Override
    public void contractDetails(int requestId, ContractDetails contractDetails) {
        Security security = fromContractDetails(contractDetails);
        Set<Security> requestResults = resultsContractDetails.get(requestId);
        requestResults.add(security);
    }

    /**
     *
     * @param currentRequestId
     */
    @Override
    public void contractDetailsEnd(int currentRequestId) {
        log.debug("received contract details end for request: {}", currentRequestId);
        requestedContracts.remove(currentRequestId);
    }

    @Override
    public void error(Exception e) {
        log.error("IBrokers callback wrapper error", e);
    }

    @Override
    public void error(String str) {
        log.error(str);
    }
}
