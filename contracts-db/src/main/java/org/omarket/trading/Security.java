package org.omarket.trading;

import com.google.gson.JsonObject;
import com.ib.client.Contract;
import com.ib.client.ContractDetails;

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

    public static Security fromJson(JsonObject contractDetails) {
        JsonObject contract = contractDetails.getAsJsonObject("m_contract");
        String currency = contract.get("m_currency").getAsString();
        String primaryExchange = contract.get("m_primaryExch").getAsString();
        String exchange = contract.get("m_exchange").getAsString();
        String securityType = contract.get("m_secType").getAsString();
        String conId = String.valueOf(contract.get("m_conid").getAsInt());
        String localSymbol = contract.get("m_localSymbol").getAsString();
        Double minTick = contractDetails.get("m_minTick").getAsDouble();
        String priceMagnifier = contractDetails.get("m_priceMagnifier").getAsString();
        String longName = contractDetails.get("m_longName").getAsString();
        return new Security(conId, localSymbol, currency, primaryExchange, exchange, securityType, minTick, priceMagnifier, longName);
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

    public String getCode() {
        return code;
    }

    public String getCurrency() {
        return currency;
    }

    public String getExchange() {
        String defaultExchange;
        if (primaryExchange != null) {
            defaultExchange = primaryExchange;
        } else {
            defaultExchange = exchange;
        }
        return defaultExchange;
    }

    public String getSecurityType() {
        return securityType;
    }

    public BigDecimal getMinTick() {
        return new BigDecimal(minTick, MathContext.DECIMAL32).stripTrailingZeros();
    }

    public ContractDetails toContractDetails() {
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

    public String toJson() {
        JsonObject contractDetails = new JsonObject();
        JsonObject contract = new JsonObject();
        contract.addProperty("m_currency", currency);
        contract.addProperty("m_primaryExch", primaryExchange);
        contract.addProperty("m_exchange", exchange);
        contract.addProperty("m_secType", securityType);
        contract.addProperty("m_conid", Integer.valueOf(code));
        contract.addProperty("m_localSymbol", symbol);
        contractDetails.add("m_contract", contract);
        contractDetails.addProperty("m_minTick", minTick);
        contractDetails.addProperty("m_longName", longName);
        contractDetails.addProperty("m_priceMagnifier", priceMagnifier);
        return contractDetails.toString();
    }

}
