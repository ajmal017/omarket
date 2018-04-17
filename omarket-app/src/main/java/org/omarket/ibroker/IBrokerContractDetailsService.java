package org.omarket.ibroker;

import com.ib.client.Contract;
import com.ib.client.ContractDetails;
import lombok.extern.slf4j.Slf4j;
import org.omarket.trading.Security;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.util.Collections;
import java.util.Set;
import java.util.TreeSet;

import static java.lang.String.format;
import static java.lang.Thread.sleep;


/**
 * Created by Christophe on 03/11/2016.
 */
@Slf4j
@Service
public class IBrokerContractDetailsService extends AbstractIBrokerClient {

    public static final int IB_MAX_SIMULTANEOUS_CONTRACT_DETAILS_REQUESTS = 10;
    public static final int PERIOD_CONTRACT_DETAILS_REQUEST_RETRY_MS = 1000;
    @Value("${address.error_message_prefix}")
    private String ADDRESS_ERROR_MESSAGE_PREFIX;

    private Set<Integer> requestsContractDetails = Collections.synchronizedSet(new TreeSet<>());

    public String getErrorChannel(Integer requestId) {
        return ADDRESS_ERROR_MESSAGE_PREFIX + "." + requestId;
    }

    /**
     *
     * @param contract
     * @return
     */
    public String requestContract(Contract contract) {
        Integer newRequestId = newRequestId();
        while(this.requestsContractDetails.size() >= IB_MAX_SIMULTANEOUS_CONTRACT_DETAILS_REQUESTS){
            try {
                sleep(PERIOD_CONTRACT_DETAILS_REQUEST_RETRY_MS);
            } catch (InterruptedException e) {
                log.error("interrupted request for contract details", e);
            }
        }
        this.requestsContractDetails.add(newRequestId);
        log.info(format("request contract details: id %s", newRequestId));
        getClientSocket().reqContractDetails(newRequestId, contract);
        return getErrorChannel(newRequestId);
    }

    @Override
    public void contractDetails(int requestId, ContractDetails contractDetails) {
        if (requestsContractDetails.contains(requestId)) {
            Security security = Security.fromContractDetails(contractDetails);
            // TODO cumulates data and returns
            requestsContractDetails.remove(requestId);
        }
    }

    @Override
    public void contractDetailsEnd(int currentRequestId) {
        log.debug("received contract details end for request: {}", currentRequestId);
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
