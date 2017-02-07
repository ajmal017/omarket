package org.omarket.trading;

import com.ib.client.Contract;
import com.ib.client.ContractDetails;
import io.vertx.core.json.Json;
import io.vertx.core.json.JsonObject;

import java.math.BigDecimal;
import java.math.MathContext;

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
    private final String priceMagnifier;
    private final String longName;

    public Security(String code, String symbol, String currency, String primaryExchange, String exchange, String securityType, Double minTick, String priceMagnifier, String longName) {
        this.code = code;
        this.symbol = symbol;
        this.currency = currency;
        this.primaryExchange = primaryExchange;
        this.exchange = exchange;
        this.securityType = securityType;
        this.minTick = minTick;
        this.priceMagnifier = priceMagnifier;
        this.longName = longName;
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
        String priceMagnifier = String.valueOf(contractDetails.getInteger("m_priceMagnifier"));
        String longName = contractDetails.getString("m_longName");
        return new Security(conId, localSymbol, currency, primaryExchange, exchange, securityType, minTick, priceMagnifier, longName);
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

    public BigDecimal getMinTick() {
        return new BigDecimal(minTick, MathContext.DECIMAL32).stripTrailingZeros();
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

    public static Security fromContractDetails(ContractDetails contractDetails) {
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

    public String toJson() {
        JsonObject contractDetails = new JsonObject();
        JsonObject contract = new JsonObject();
        contract.put("m_currency", currency);
        contract.put("m_primaryExch", primaryExchange);
        contract.put("m_exchange", exchange);
        contract.put("m_secType", securityType);
        contract.put("m_conid", Integer.valueOf(code));
        contract.put("m_localSymbol", symbol);
        contractDetails.put("m_contract", contract);
        contractDetails.put("m_minTick", minTick);
        contractDetails.put("m_longName", longName);
        contractDetails.put("m_priceMagnifier", priceMagnifier);
        return Json.encode(contractDetails);
    }
}
