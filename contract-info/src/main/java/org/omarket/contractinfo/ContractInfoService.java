package org.omarket.contractinfo;

import org.omarket.quotes.Security;

import javax.jws.WebMethod;
import javax.jws.WebService;

@WebService
public interface ContractInfoService {
    @WebMethod
    public Security getContractInfo(String code);
}
