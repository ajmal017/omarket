package org.omarket.trading;

import com.ib.client.Contract;
import com.ib.client.ContractDetails;
import io.vertx.core.json.JsonObject;

/**
 * Created by christophe on 09/01/17.
 */
public class Security {
    private final String code;
    private final String symbol;
    private final String currency;
    private final String primaryExchange;
    private final String exchange;
    private final String securityType;
    private final Double minTick;

    public Security(String code, String symbol, String currency, String primaryExchange, String exchange, String securityType, Double minTick) {
        this.code = code;
        this.symbol = symbol;
        this.currency = currency;
        this.primaryExchange = primaryExchange;
        this.exchange = exchange;
        this.securityType = securityType;
        this.minTick = minTick;
    }

    public static Security fromJson(JsonObject contractDetails){
        JsonObject contract = contractDetails.getJsonObject("m_contract");
        String currency = contract.getString("m_currency");
        String primaryExchange = contract.getString("m_primaryExch");
        String exchange = contract.getString("m_exchange");
        String securityType = contract.getString("m_secType");
        String conId = String.valueOf(contract.getInteger("m_conid"));
        String localSymbol = contract.getString("m_localSymbol");
        Double minTick = contractDetails.getDouble("m_minTick");
        return new Security(conId, localSymbol, currency, primaryExchange, exchange, securityType, minTick);
    }

    public String getCode(){
        return code;
    }

    public String getCurrency(){
        return currency;
    }
    public String getExchange(){
        String defaultExchange;
        if(primaryExchange != null){
            defaultExchange = primaryExchange;
        } else {
            defaultExchange = exchange;
        }
        return defaultExchange;
    }
    public String getSecurityType(){
        return securityType;
    }

    public Double minTick() {
        return minTick;
    }

    public ContractDetails toContractDetails(){
        ContractDetails ibContractDetails = new ContractDetails();
        ibContractDetails.minTick(minTick);
        Contract ibContract = new Contract();
        ibContract.conid(Integer.valueOf(code));
        ibContract.currency(currency);
        ibContract.primaryExch(primaryExchange);
        ibContract.secType(securityType);
        ibContract.exchange(exchange);
        ibContract.localSymbol(symbol);
        ibContractDetails.contract(ibContract);
        return ibContractDetails;
    }

    public String getSymbol() {
        return symbol;
    }
}
