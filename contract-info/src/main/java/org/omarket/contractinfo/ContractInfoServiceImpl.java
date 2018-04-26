package org.omarket.contractinfo;

import javax.jws.WebService;

import com.ib.client.Contract;
import org.omarket.ibroker.IBrokerContractDetailsService;
import org.omarket.quotes.Security;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

@Service
public class ContractInfoServiceImpl implements ContractInfoService {

    private final IBrokerContractDetailsService contractDetailsService;

    @Autowired
    public ContractInfoServiceImpl(IBrokerContractDetailsService contractDetailsService){
        this.contractDetailsService = contractDetailsService;
    }

    @Override
    public Security getContractInfo(String code) {
        contractDetailsService.requestContractDetails(new Contract());
        return null;
    }
}
