package org.omarket.trading;

import com.ib.client.Contract;
import com.ib.client.ContractDetails;
import io.vertx.core.json.JsonObject;

/**
 * Created by christophe on 09/01/17.
 */
public class Security {
    private ContractDetails underlying;

    public Security(ContractDetails contractDetails){
        underlying = contractDetails;
    }

    public static Security fromJson(JsonObject contractDetails){
        JsonObject contract = contractDetails.getJsonObject("m_contract");
        String currency = contract.getString("m_currency");
        String primaryExchange = contract.getString("m_primaryExch");
        String exchange = contract.getString("m_exchange");
        String securityType = contract.getString("m_secType");
        Integer conId = contract.getInteger("m_conid");
        ContractDetails ibContractDetails = new ContractDetails();
        Contract ibContract = new Contract();
        ibContract.conid(conId);
        ibContract.currency(currency);
        ibContract.primaryExch(primaryExchange);
        ibContract.secType(securityType);
        ibContract.exchange(exchange);
        ibContractDetails.contract(ibContract);
        return new Security(ibContractDetails);
    }

    public String getCode(){
        return String.valueOf(underlying.conid());
    }

    public String getCurrency(){
        return underlying.contract().currency();
    }
    public String getExchange(){
        String exchange = null;
        if(underlying.contract().primaryExch() != null){
            exchange = underlying.contract().primaryExch();
        } else {
            exchange = underlying.contract().exchange();
        }
        return exchange;
    }
    public String getSecurityType(){
        return underlying.contract().getSecType();
    }

    public Double minTick() {
        return underlying.minTick();
    }

    public ContractDetails toContractDetails(){
        return underlying;
    }

    public String getSymbol() {
        return underlying.contract().localSymbol();
    }
}
