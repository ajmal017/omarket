package ibdemo.samples.testbed.orders;

import java.util.ArrayList;
import java.util.List;
import com.ib.client.Order;
import com.ib.client.OrderComboLeg;
import com.ib.client.OrderType;
import com.ib.client.TagValue;

public class OrderSamples {
	
	public static Order AtAuction(String action, double quantity, double price) {
		//! [auction]
		Order order = new Order();
		order.action(action);
		order.tif("AUC");
		order.orderType("MTL");
		order.totalQuantity((int)quantity);
		order.lmtPrice(price);
		//! [auction]
		return order;
	}
	
	public static Order Discretionary(String action, double quantity, double price, double discretionaryAmt) {
		//! [discretionary]
		Order order = new Order();
		order.action(action);
		order.orderType("LMT");
		order.totalQuantity((int)quantity);
		order.lmtPrice(price);
		order.discretionaryAmt(discretionaryAmt);
		//! [discretionary]
		return order;
	}

	public static Order MarketOrder(String action, double quantity) {
		//! [market]
		Order order = new Order();
		order.action(action);
		order.orderType("MKT");
		order.totalQuantity((int)quantity);
		//! [market]
		return order;
	}
	
	public static Order MarketIfTouched(String action, double quantity, double price) {
		//! [market_if_touched]
		Order order = new Order();
		order.action(action);
		order.orderType("MIT");
		order.totalQuantity((int)quantity);
		order.auxPrice(price);
		//! [market_if_touched]
		return order;
	}
	
	public static Order MarketOnClose(String action, double quantity) {
		//! [market_on_close]
		Order order = new Order();
		order.action(action);
		order.orderType("MOC");
		order.totalQuantity((int)quantity);
		//! [market_on_close]
		return order;
	}
	
	public static Order MarketOnOpen(String action, double quantity) {
		//! [market_on_open]
		Order order = new Order();
		order.action(action);
		order.orderType("MKT");
		order.totalQuantity((int)quantity);
		order.tif("OPG");
		//! [market_on_open]
		return order;
	}
	
	public static Order MidpointMatch(String action, double quantity) {
		//! [midpoint_match]
		Order order = new Order();
		order.action(action);
		order.orderType("MKT");
		order.totalQuantity((int)quantity);
		//! [midpoint_match]
		return order;
	}

	public static Order PeggedToMarket(String action, double quantity, double marketOffset) {
		//! [pegged_market]
		Order order = new Order();
		order.action(action);
		order.orderType("PEG MKT");
		order.totalQuantity(100);
		order.auxPrice(marketOffset);//Offset price
		//! [pegged_market]
		return order;
	}
	
	public static Order PeggedToStock(String action, double quantity, double delta, double stockReferencePrice, double startingPrice) {
		//! [pegged_stock]
		Order order = new Order();
		order.action(action);
		order.orderType("PEG STK");
		order.totalQuantity((int)quantity);
		order.delta(delta);
		order.lmtPrice(stockReferencePrice);
		order.startingPrice(startingPrice);
		//! [pegged_stock]
		return order;
	}
	
	public static Order RelativePeggedToPrimary(String action, double quantity, double priceCap, double offsetAmount) {
		//! [relative_pegged_primary]
		Order order = new Order();
		order.action(action);
		order.orderType("REL");
		order.totalQuantity((int)quantity);
		order.lmtPrice(priceCap);
		order.auxPrice(offsetAmount);
		//! [relative_pegged_primary]
		return order;
	}
	
	public static Order SweepToFill(String action, double quantity, double price) {
		//! [sweep_to_fill]
		Order order = new Order();
		order.action(action);
		order.orderType("LMT");
		order.totalQuantity((int)quantity);
		order.lmtPrice(price);
		order.sweepToFill(true);
		//! [sweep_to_fill]
		return order;
	}
	
	public static Order AuctionLimit(String action, double quantity, double price, int auctionStrategy) {
		//! [auction_limit]
		Order order = new Order();
		order.action(action);
		order.orderType("LMT");
		order.totalQuantity((int)quantity);
		order.lmtPrice(price);
		order.auctionStrategy(auctionStrategy);
		//! [auction_limit]
		return order;
	}
	
	public static Order AuctionPeggedToStock(String action, double quantity, double startingPrice, double delta) {
		//! [auction_pegged_stock]
		Order order = new Order();
		order.action(action);
		order.orderType("PEG STK");
		order.totalQuantity((int)quantity);
		order.delta(delta);
		order.startingPrice(startingPrice);
		//! [auction_pegged_stock]
		return order;
	}
	
	public static Order AuctionRelative(String action, double quantity, double offset) {
		//! [auction_relative]
		Order order = new Order();
		order.action(action);
		order.orderType("REL");
		order.totalQuantity((int)quantity);
		order.auxPrice(offset);
		//! [auction_relative]
		return order;
	}
	
	public static Order Block(String action, double quantity, double price) {
		// ! [block]
		Order order = new Order();
		order.action(action);
		order.orderType("LMT");
		order.totalQuantity((int)quantity);//Large volumes!
		order.lmtPrice(price);
		order.blockOrder(true);
		// ! [block]
		return order;
	}
	
	public static Order BoxTop(String action, double quantity) {
		// ! [boxtop]
		Order order = new Order();
		order.action(action);
		order.orderType("BOX TOP");
		order.totalQuantity((int)quantity);
		// ! [boxtop]
		return order;
	}
	
	public static Order LimitOrder(String action, double quantity, double limitPrice) {
		// ! [limitorder]
		Order order = new Order();
		order.action(action);
		order.orderType("LMT");
		order.totalQuantity((int)quantity);
		order.lmtPrice(limitPrice);
		// ! [limitorder]
		return order;
	}
	
	public static Order LimitIfTouched(String action, double quantity, double limitPrice, double triggerPrice) {
		// ! [limitiftouched]
		Order order = new Order();
		order.action(action);
		order.orderType("LIT");
		order.totalQuantity((int)quantity);
		order.lmtPrice(limitPrice);
		order.auxPrice(triggerPrice);
		// ! [limitiftouched]
		return order;
	}
	
	public static Order LimitOnClose(String action, double quantity, double limitPrice) {
		// ! [limitonclose]
		Order order = new Order();
		order.action(action);
		order.orderType("LOC");
		order.totalQuantity((int)quantity);
		order.lmtPrice(limitPrice);
		// ! [limitonclose]
		return order;
	}
	
	public static Order LimitOnOpen(String action, double quantity, double limitPrice) {
		// ! [limitonopen]
		Order order = new Order();
		order.action(action);
		order.tif("OPG");
		order.orderType("LOC");
		order.totalQuantity((int)quantity);
		order.lmtPrice(limitPrice);
		// ! [limitonopen]
		return order;
	}
	
	public static Order PassiveRelative(String action, double quantity, double offset) {
		// ! [passive_relative]
		Order order = new Order();
		order.action(action);
		order.orderType("PASSV REL");
		order.totalQuantity((int)quantity);
		order.auxPrice(offset);
		// ! [passive_relative]
		return order;
	}
	
	public static Order PeggedToMidpoint(String action, double quantity, double offset) {
		// ! [pegged_midpoint]
		Order order = new Order();
		order.action(action);
		order.orderType("PEG MID");
		order.totalQuantity((int)quantity);
		order.auxPrice(offset);
		// ! [pegged_midpoint]
		return order;
	}
	
	//! [bracket]
	public static List<Order> BracketOrder(int parentOrderId, String action, double quantity, double limitPrice, double takeProfitLimitPrice, double stopLossPrice) {
		//This will be our main or "parent" order
		Order parent = new Order();
		parent.orderId(parentOrderId);
		parent.action(action);
		parent.orderType("LMT");
		parent.totalQuantity((int)quantity);
		parent.lmtPrice(limitPrice);
		//The parent and children orders will need this attribute set to false to prevent accidental executions.
        //The LAST CHILD will have it set to true.
		parent.transmit(false);
		
		Order takeProfit = new Order();
		takeProfit.orderId(parent.orderId() + 1);
		takeProfit.action(action.equals("BUY") ? "SELL" : "BUY");
		takeProfit.orderType("LMT");
		takeProfit.totalQuantity((int)quantity);
		takeProfit.lmtPrice(takeProfitLimitPrice);
		takeProfit.parentId(parentOrderId);
		takeProfit.transmit(false);
		
		Order stopLoss = new Order();
		stopLoss.orderId(parent.orderId() + 2);
		stopLoss.action(action.equals("BUY") ? "SELL" : "BUY");
		stopLoss.orderType("STP");
		//Stop trigger price
		stopLoss.auxPrice(stopLossPrice);
		stopLoss.totalQuantity((int)quantity);
		stopLoss.parentId(parentOrderId);
		//In this case, the low side order will be the last child being sent. Therefore, it needs to set this attribute to true 
        //to activate all its predecessors
		stopLoss.transmit(true);
		
		List<Order> bracketOrder = new ArrayList<Order>();
		bracketOrder.add(parent);
		bracketOrder.add(takeProfit);
		bracketOrder.add(stopLoss);
		
		return bracketOrder;
	}
	//! [bracket]
	
	public static Order MarketToLimit(String action, double quantity) {
		// ! [markettolimit]
		Order order = new Order();
		order.action(action);
		order.orderType("MTL");
		order.totalQuantity((int)quantity);
		// ! [markettolimit]
		return order;
	}
	
	public static Order MarketWithProtection(String action, double quantity) {
		// ! [marketwithprotection]
		Order order = new Order();
		order.action(action);
		order.orderType("MKT PRT");
		order.totalQuantity((int)quantity);
		// ! [marketwithprotection]
		return order;
	}
	
	public static Order Stop(String action, double quantity, double stopPrice) {
		// ! [stop]
		Order order = new Order();
		order.action(action);
		order.orderType("STP");
		order.auxPrice(stopPrice);
		order.totalQuantity((int)quantity);
		// ! [stop]
		return order;
	}
	
	public static Order StopLimit(String action, double quantity, double limitPrice, double stopPrice) {
		// ! [stoplimit]
		Order order = new Order();
		order.action(action);
		order.orderType("STP LMT");
		order.lmtPrice(limitPrice);
		order.auxPrice(stopPrice);
		order.totalQuantity((int)quantity);
		// ! [stoplimit]
		return order;
	}
	
	public static Order StopWithProtection(String action, double quantity, double stopPrice) {
		// ! [stopwithprotection]
		Order order = new Order();
		order.action(action);
		order.orderType("STP PRT");
		order.auxPrice(stopPrice);
		order.totalQuantity((int)quantity);
		// ! [stopwithprotection]
		return order;
	}
	
	public static Order TrailingStop(String action, double quantity, double trailingPercent, double trailStopPrice) {
		// ! [trailingstop]
		Order order = new Order();
		order.action(action);
		order.orderType("TRAIL");
		order.trailingPercent(trailingPercent);
		order.trailStopPrice(trailStopPrice);
		order.totalQuantity((int)quantity);
		// ! [trailingstop]
		return order;
	}
	
	public static Order TrailingStopLimit(String action, double quantity, double trailingAmount, double trailStopPrice, double limitPrice) {
		// ! [trailingstoplimit]
		Order order = new Order();
		order.action(action);
		order.orderType("TRAIL LIMIT");
		order.lmtPrice(limitPrice);
		order.auxPrice(trailingAmount);
		order.trailStopPrice(trailStopPrice);
		order.totalQuantity((int)quantity);
		// ! [trailingstoplimit]
		return order;
	}
	
	public static Order ComboLimitOrder(String action, double quantity, boolean nonGuaranteed, double limitPrice) {
		// ! [combolimit]
		Order order = new Order();
		order.action(action);
		order.orderType("LMT");
		order.lmtPrice(limitPrice);
		order.totalQuantity((int)quantity);
		if (nonGuaranteed)
		{
			List<TagValue> smartComboRoutingParams = new ArrayList<TagValue>();
			smartComboRoutingParams.add(new TagValue("NonGuaranteed", "1"));
		}
		// ! [combolimit]
		return order;
	}
	
	public static Order ComboMarketOrder(String action, double quantity, boolean nonGuaranteed) {
		// ! [combomarket]
		Order order = new Order();
		order.action(action);
		order.orderType("MKT");
		order.totalQuantity((int)quantity);
		if (nonGuaranteed)
		{
			List<TagValue> smartComboRoutingParams = new ArrayList<TagValue>();
			smartComboRoutingParams.add(new TagValue("NonGuaranteed", "1"));
		}
		// ! [combomarket]
		return order;
	}
	
	public static Order LimitOrderForComboWithLegPrices(String action, double quantity, boolean nonGuaranteed, double[] legPrices) {
		// ! [limitordercombolegprices]
		Order order = new Order();
		order.action(action);
		order.orderType("LMT");
		order.totalQuantity((int)quantity);
		order.orderComboLegs(new ArrayList<OrderComboLeg>());
		
		for(double price : legPrices) {
			OrderComboLeg comboLeg = new OrderComboLeg();
			comboLeg.price(5.0);
			order.orderComboLegs().add(comboLeg);
		}
		
		if (nonGuaranteed)
		{
			List<TagValue> smartComboRoutingParams = new ArrayList<TagValue>();
			smartComboRoutingParams.add(new TagValue("NonGuaranteed", "1"));
		}
		// ! [limitordercombolegprices]
		return order;
	}
	
	public static Order RelativeLimitCombo(String action, double quantity, boolean nonGuaranteed, double limitPrice) {
		// ! [relativelimitcombo]
		Order order = new Order();
		order.action(action);
		order.orderType("REL + LMT");
		order.totalQuantity((int)quantity);
		order.lmtPrice(limitPrice);
		if (nonGuaranteed)
		{
			List<TagValue> smartComboRoutingParams = new ArrayList<TagValue>();
			smartComboRoutingParams.add(new TagValue("NonGuaranteed", "1"));
		}
		// ! [relativelimitcombo]
		return order;
	}
	
	public static Order RelativeMarketCombo(String action, double quantity, boolean nonGuaranteed) {
		// ! [relativemarketcombo]
		Order order = new Order();
		order.action(action);
		order.orderType("REL + MKT");
		order.totalQuantity((int)quantity);
		if (nonGuaranteed)
		{
			List<TagValue> smartComboRoutingParams = new ArrayList<TagValue>();
			smartComboRoutingParams.add(new TagValue("NonGuaranteed", "1"));
		}
		// ! [relativemarketcombo]
		return order;
	}
	
	// ! [oca]
	public static List<Order> OneCancelsAll(String ocaGroup, List<Order> ocaOrders, int ocaType) {
		
		for (Order o : ocaOrders) {
			o.ocaGroup(ocaGroup);
			o.ocaType(ocaType);
		}
		return ocaOrders;
	}
	// ! [oca]
	
	public static Order Volatility(String action, double quantity, double volatilityPercent, int volatilityType) {
		// ! [volatility]
		Order order = new Order();
		order.action(action);
		order.orderType("VOL");
		order.volatility(volatilityPercent);//Expressed in percentage (40%)
		order.volatilityType(volatilityType);// 1=daily, 2=annual
		order.totalQuantity((int)quantity);
		// ! [volatility]
		return order;
	}
	
	//! [fhedge]
	public static Order MarketFHedge(int parentOrderId, String action) {
		//FX Hedge orders can only have a quantity of 0
		Order order = MarketOrder(action, 0);
		order.parentId(parentOrderId);
		order.hedgeType("F");
		return order;
	}
	//! [fhedge]
	
	public static Order PeggedToBenchmark(String action, double quantity, double startingPrice, boolean peggedChangeAmountDecrease, double peggedChangeAmount, double referenceChangeAmount, int referenceConId, String referenceExchange, double stockReferencePrice,  
            double referenceContractLowerRange, double referenceContractUpperRange) {
		//! [pegged_benchmark]
		Order order = new Order();
		order.orderType("PEG BENCH");
		//BUY or SELL
		order.action(action);
		order.totalQuantity((int)quantity);
		//Beginning with price...
		order.startingPrice(startingPrice);
		//starting reference price is...
		order.stockRefPrice(stockReferencePrice);
		//Keep order active as long as reference contract trades between...
		order.stockRangeLower(referenceContractLowerRange);
		//and...
		order.stockRangeUpper(referenceContractUpperRange);
		//! [pegged_benchmark]
		return order;
	}
	
    public static Order AttachAdjustableToStop(Order parent, double attachedOrderStopPrice, double triggerPrice, double adjustStopPrice) {
        //! [adjustable_stop]
        Order order = new Order();
        //Attached order is a conventional STP order in opposite direction
        order.action(parent.action().equals("BUY") ? "SELL" : "BUY");
        order.totalQuantity(parent.totalQuantity());
        order.auxPrice(attachedOrderStopPrice);
        order.parentId(parent.orderId());
        return order;
    }
	
}