package org.omarket.trading.ibrokers;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

import static org.omarket.trading.MarketData.createChannelQuote;

/**
 * Created by Christophe on 05/11/2016.
 */
public class CurrencyProduct {
    private final static Logger logger = LoggerFactory.getLogger(CurrencyProduct.class);
    public final static Map<String, Integer> IB_CODES = new HashMap<>();
    static {
        IB_CODES.put("AUD.ZAR", 208558338);
        IB_CODES.put("GBP.ZAR", 208558332);
        IB_CODES.put("EUR.ZAR", 208558343);
        IB_CODES.put("CHF.ZAR", 208558349);
        IB_CODES.put("USD.ZAR", 44984973);
        IB_CODES.put("AUD.USD", 14433401);
        IB_CODES.put("GBP.USD", 12087797);
        IB_CODES.put("EUR.USD", 12087792);
        IB_CODES.put("KRW.USD", 27717934);
        IB_CODES.put("NZD.USD", 39453441);
        IB_CODES.put("CHF.USD", 12087802);
        IB_CODES.put("AUD.SGD", 61664938);
        IB_CODES.put("EUR.SGD", 37928764);
        IB_CODES.put("USD.SGD", 37928772);
        IB_CODES.put("GBP.SEK", 28027122);
        IB_CODES.put("DKK.SEK", 28027110);
        IB_CODES.put("EUR.SEK", 37893488);
        IB_CODES.put("NOK.SEK", 28027113);
        IB_CODES.put("CHF.SEK", 37893493);
        IB_CODES.put("USD.SEK", 37893486);
        IB_CODES.put("EUR.RUB", 92620186);
        IB_CODES.put("USD.RUB", 28454968);
        IB_CODES.put("EUR.PLN", 75015682);
        IB_CODES.put("AUD.NZD", 39453424);
        IB_CODES.put("GBP.NZD", 47101305);
        IB_CODES.put("EUR.NZD", 47101302);
        IB_CODES.put("GBP.NOK", 37943445);
        IB_CODES.put("DKK.NOK", 110616599);
        IB_CODES.put("EUR.NOK", 39453434);
        IB_CODES.put("CHF.NOK", 37943440);
        IB_CODES.put("USD.NOK", 37971206);
        IB_CODES.put("EUR.MXN", 37890904);
        IB_CODES.put("USD.MXN", 35045199);
        IB_CODES.put("USD.KRW", 36363302);
        IB_CODES.put("AUD.JPY", 15016133);
        IB_CODES.put("GBP.JPY", 14321015);
        IB_CODES.put("CAD.JPY", 15016241);
        IB_CODES.put("CNH.JPY", 114900065);
        IB_CODES.put("DKK.JPY", 110616600);
        IB_CODES.put("EUR.JPY", 14321016);
        IB_CODES.put("HKD.JPY", 15016090);
        IB_CODES.put("KRW.JPY", 36771223);
        IB_CODES.put("MXN.JPY", 37890903);
        IB_CODES.put("NZD.JPY", 39453444);
        IB_CODES.put("NOK.JPY", 110616605);
        IB_CODES.put("SGD.JPY", 61664943);
        IB_CODES.put("ZAR.JPY", 208558344);
        IB_CODES.put("SEK.JPY", 37890923);
        IB_CODES.put("CHF.JPY", 14321010);
        IB_CODES.put("USD.JPY", 15016059);
        IB_CODES.put("EUR.ILS", 44495104);
        IB_CODES.put("USD.ILS", 44495102);
        IB_CODES.put("EUR.HUF", 75015675);
        IB_CODES.put("USD.HUF", 34831484);
        IB_CODES.put("AUD.HKD", 15016128);
        IB_CODES.put("GBP.HKD", 12345775);
        IB_CODES.put("CAD.HKD", 15016239);
        IB_CODES.put("CNH.HKD", 174179663);
        IB_CODES.put("EUR.HKD", 12345770);
        IB_CODES.put("KRW.HKD", 36771218);
        IB_CODES.put("USD.HKD", 12345777);
        IB_CODES.put("EUR.GBP", 12087807);
        IB_CODES.put("KRW.GBP", 36771212);
        IB_CODES.put("KRW.EUR", 36771209);
        IB_CODES.put("GBP.DKK", 110616560);
        IB_CODES.put("EUR.DKK", 39394687);
        IB_CODES.put("CHF.DKK", 37943438);
        IB_CODES.put("EUR.CZK", 75015678);
        IB_CODES.put("USD.CZK", 34838409);
        IB_CODES.put("AUD.CNH", 114900056);
        IB_CODES.put("GBP.CNH", 114900044);
        IB_CODES.put("CAD.CNH", 114900050);
        IB_CODES.put("EUR.CNH", 114900041);
        IB_CODES.put("SGD.CNH", 114900061);
        IB_CODES.put("CHF.CNH", 114900055);
        IB_CODES.put("USD.CNH", 113342317);
        IB_CODES.put("AUD.CHF", 15016125);
        IB_CODES.put("GBP.CHF", 12087826);
        IB_CODES.put("CAD.CHF", 15016234);
        IB_CODES.put("EUR.CHF", 12087817);
        IB_CODES.put("KRW.CHF", 36771206);
        IB_CODES.put("NZD.CHF", 46189224);
        IB_CODES.put("USD.CHF", 12087820);
        IB_CODES.put("AUD.CAD", 15016138);
        IB_CODES.put("GBP.CAD", 15016078);
        IB_CODES.put("EUR.CAD", 15016068);
        IB_CODES.put("KRW.CAD", 36771203);
        IB_CODES.put("NZD.CAD", 46189223);
        IB_CODES.put("USD.CAD", 15016062);
        IB_CODES.put("GBP.AUD", 15016075);
        IB_CODES.put("EUR.AUD", 15016065);
        IB_CODES.put("KRW.AUD", 36771196);
    }

    public String getChannel(String currency1, String currency2){
        String cross = currency1.toUpperCase() + "." + currency2.toUpperCase();
        if (!IB_CODES.containsKey(cross)){
            logger.error("no channel for {} / {}", currency1, currency2);
        }
        return createChannelQuote(IB_CODES.get(cross).toString());
    }

    public String getChannelDirect(String currency){
        return getChannel(currency, "USD");
    }

    public String getChannelIndirect(String currency){
        return getChannel("USD", currency);
    }
}
