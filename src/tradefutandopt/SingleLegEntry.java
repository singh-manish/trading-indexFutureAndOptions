/*
 The MIT License (MIT)

 Copyright (c) 2015 Manish Kumar Singh

 Permission is hereby granted, free of charge, to any person obtaining a copy
 of this software and associated documentation files (the "Software"), to deal
 in the Software without restriction, including without limitation the rights
 to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 copies of the Software, and to permit persons to whom the Software is
 furnished to do so, subject to the following conditions:

 The above copyright notice and this permission notice shall be included in all
 copies or substantial portions of the Software.

 THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 SOFTWARE.
 
 */
package tradefutandopt;

import java.util.Calendar;
import java.util.TimeZone;
import redis.clients.jedis.*;

/**
 * @author Manish Kumar Singh
 */
public class SingleLegEntry extends Thread {

    private String threadName = "TakingNewPositionThread";
    private String legDetails;
    private boolean debugFlag;
    private JedisPool jedisPool;
    private IBInteraction ibInteractionClient;
    private TimeZone exchangeTimeZone;
    private MyUtils myUtils;
    private String strategyName;
    private String openPositionsQueueKeyName;
    private String futExpiry;
    private String legName;
    private int legLotSize;
    private String legPosition; // "BUY" for long. "SELL" for short.
    private String orderTypeToUse; // market, relativewithzeroaslimitwithamountoffset, relativewithmidpointaslimitwithamountoffset, relativewithzeroaslimitwithpercentoffset, relativewithmidpointaslimitwithpercentoffset 
    private String contractTypeToUse = "FUT"; // FUT or STK or OPT
    private String monitoringContractTypeToUse = "IND"; // IND or FUT or STK    
    private String rightTypeToUse = "CALL"; // "C" or "CALL" or "P" or "PUT" - for options
    private double strikePriceToUse = 8000.0; // strike price for options - for options    

    private int INITIALSTOPLOSSAMOUNT = 20000;
    private int slotNumber = 3;
    private int legSizeMultiple = 1;

    SingleLegEntry(String name, String legInfo, JedisPool redisConnectionPool, IBInteraction ibInterClient, MyUtils utils, String strategyReference, String openPosQueueName, TimeZone exTZ, String confOrderType, int assignedSlotNumber, int stopLossAmount, String exCurrency, boolean debugIndicator) {
        threadName = name;
        legDetails = legInfo;
        debugFlag = debugIndicator;
        jedisPool = redisConnectionPool;

        ibInteractionClient = ibInterClient;
        exchangeTimeZone = exTZ;

        myUtils = utils;

        strategyName = strategyReference;
        openPositionsQueueKeyName = openPosQueueName;
        orderTypeToUse = confOrderType;
        slotNumber = assignedSlotNumber;
        INITIALSTOPLOSSAMOUNT = stopLossAmount;
        TimeZone.setDefault(exchangeTimeZone);

        TradingObject myTradeObject = new TradingObject(legDetails);
        legSizeMultiple = Math.abs(myTradeObject.getSideAndSize());
        
        contractTypeToUse = myTradeObject.getTradingContractType();
        monitoringContractTypeToUse = myTradeObject.getMonitoringContractType();        
        if (contractTypeToUse.equalsIgnoreCase("FUT")) {
            if (exCurrency.equalsIgnoreCase("inr")) {
                // for FUT type of action, fut expiry needs to be defined
                futExpiry = myUtils.getKeyValueFromRedis(jedisPool, "INRFUTCURRENTEXPIRY", false);
            } else if (exCurrency.equalsIgnoreCase("usd")) {
                // for FUT type of action, fut expiry needs to be defined
                futExpiry = myUtils.getKeyValueFromRedis(jedisPool, "USDFUTCURRENTEXPIRY", false);
            }
        } else if (contractTypeToUse.equalsIgnoreCase("OPT")) {
            if (exCurrency.equalsIgnoreCase("inr")) {
                // for OPT type of action, fut expiry needs to be defined
                futExpiry = myUtils.getKeyValueFromRedis(jedisPool, "INROPTCURRENTEXPIRY", false);
            } else if (exCurrency.equalsIgnoreCase("usd")) {
                // for OPT type of action, fut expiry needs to be defined
                futExpiry = myUtils.getKeyValueFromRedis(jedisPool, "USDOPTCURRENTEXPIRY", false);
            }
        } else if (contractTypeToUse.equalsIgnoreCase("STK")) {
            // for STK type of action, mark futexpiry to 00000000
            futExpiry = "00000000";
        }        
        legName = myTradeObject.getTradingContractUnderlyingName();        
        if (myTradeObject.getSideAndSize() > 0) {
            legPosition = "BUY";
            legLotSize = Math.abs(myTradeObject.getTradingContractLotSize() * myTradeObject.getSideAndSize());
        } else if (myTradeObject.getSideAndSize() < 0) {
            legPosition = "SELL";
            legLotSize = Math.abs(myTradeObject.getTradingContractLotSize() * myTradeObject.getSideAndSize());
        } else {
            // TBD - Write error handling code here
            legPosition = "NONE";
            legLotSize = 0;            
        }
        if (contractTypeToUse.equalsIgnoreCase("OPT")) {
            rightTypeToUse = myTradeObject.getTradingContractOptionRightType();
            strikePriceToUse = myTradeObject.getTradingContractOptionStrike();
        }
    }

    boolean entryOrderCompletelyFilled(int orderId, int maxWaitTime) {

        boolean returnValue = false;
        //ibInteractionClient.ibClient.reqOpenOrders();
        int timeOut = 10;
        myUtils.waitForNSeconds(timeOut);
        while ( (!(ibInteractionClient.myOrderStatusDetails.containsKey(orderId)))
                && (timeOut < maxWaitTime)) {               
            if (debugFlag) {
                System.out.println(String.format("%1$tY%1$tm%1$td:%1$tH:%1$tM:%1$tS ", Calendar.getInstance(exchangeTimeZone)) + "Waiting for Order to be filled for Order ids " + orderId + " for " + timeOut + " seconds. Status not updated on IB yet.");  
            }
            timeOut += 5;
            // Check if following needs to be commented
            ibInteractionClient.ibClient.reqOpenOrders();
            myUtils.waitForNSeconds(5);            
        } 
        if (ibInteractionClient.myOrderStatusDetails.containsKey(orderId)) {
            while ((ibInteractionClient.myOrderStatusDetails.get(orderId).getRemainingQuantity() != 0)
                    && (timeOut < maxWaitTime)) {
                if (debugFlag) {
                    if (ibInteractionClient.myOrderStatusDetails.containsKey(orderId)) {
                        System.out.println(String.format("%1$tY%1$tm%1$td:%1$tH:%1$tM:%1$tS ", Calendar.getInstance(exchangeTimeZone)) + "Waiting for Order to be filled for Order ids " + ibInteractionClient.myOrderStatusDetails.get(orderId).getOrderId() + " for " + timeOut + " seconds");                    
                    } else {
                        System.out.println(String.format("%1$tY%1$tm%1$td:%1$tH:%1$tM:%1$tS ", Calendar.getInstance(exchangeTimeZone)) + "Waiting for Order to be filled for Order ids " + orderId + " for " + timeOut + " seconds. Status not updated on IB yet.");                                        
                    }
                }
                timeOut += 10;
                // Check if following needs to be commented
                ibInteractionClient.ibClient.reqOpenOrders();
                myUtils.waitForNSeconds(10);
            }
            if ((ibInteractionClient.myOrderStatusDetails.get(orderId).getRemainingQuantity() == 0)) {
                returnValue =  true;
            } else {
                returnValue = false;
            }            
        } else {
            returnValue = false;            
        }       

        return (returnValue);
    }

    void updateOpenPositionsQueue(String queueKeyName, String updateDetails, String orderStatus, double tradingContractSpread, int entryOrderId, int slotNumber, String bidAskPriceDetails, double monitoringContractPrice) {

        TradingObject myTradeObject = new TradingObject(updateDetails);

        if (orderStatus.equalsIgnoreCase("entryorderinitiated")) {
            myTradeObject.setEntryTimeStamp(String.format("%1$tY%1$tm%1$td%1$tH%1$tM%1$tS", Calendar.getInstance(exchangeTimeZone)));
            myTradeObject.initiateAndValidate();
        }

        if (orderStatus.equalsIgnoreCase("entryorderfilled")) {
            myTradeObject.setEntryTimeStamp(String.format("%1$tY%1$tm%1$td%1$tH%1$tM%1$tS", Calendar.getInstance(exchangeTimeZone)));
        }

        if (Math.abs(myTradeObject.getSideAndSize()) == 1) {
            myTradeObject.setSideAndSize(myTradeObject.getSideAndSize(), legSizeMultiple);            
        }
        
        if ((tradingContractSpread > 0) || (tradingContractSpread < 0)) {
            myTradeObject.setTradingContractEntrySpread(tradingContractSpread);
            // Reset the standard Deviation OR 1% of entry to actual 1% value. 
            // Entry signal contains approximate value but now actual value is known
            myTradeObject.setTradingContractEntryStdDev(Math.abs(Math.round(tradingContractSpread/100)));
        }

        myTradeObject.setMonitoringContractEntryPrice(monitoringContractPrice);
        // Reset the standard Deviation OR 1% of entry to actual 1% value. 
        // Entry signal contains approximate value but now actual value is known
        myTradeObject.setMonitoringContractEntryStdDev(Math.abs(Math.round(monitoringContractPrice/100)));
        
        if (myTradeObject.getTradingContractType().equalsIgnoreCase("OPT")) {
            myTradeObject.setSideAndSize(Math.abs(myTradeObject.getSideAndSize()), 1);
            if (myTradeObject.getTradingContractOptionRightType().equalsIgnoreCase("CALL") || myTradeObject.getTradingContractOptionRightType().equalsIgnoreCase("C")) {
                myTradeObject.setMonitoringContractLowerBreach(
                        Math.round(Float.parseFloat(myTradeObject.getMonitoringContractEntryPrice()))
                        - INITIALSTOPLOSSAMOUNT
                );
                myTradeObject.setMonitoringContractUpperBreach(
                        (int) Math.round(Float.parseFloat(myTradeObject.getMonitoringContractEntryPrice()))
                        + Math.round(Math.abs(2 * Float.parseFloat(myTradeObject.getMonitoringContractEntryStdDev())))
                );
            } else if (myTradeObject.getTradingContractOptionRightType().equalsIgnoreCase("PUT") || myTradeObject.getTradingContractOptionRightType().equalsIgnoreCase("P")) {
                myTradeObject.setMonitoringContractLowerBreach(
                        Math.round(Float.parseFloat(myTradeObject.getMonitoringContractEntryPrice()))
                        + INITIALSTOPLOSSAMOUNT
                );
                myTradeObject.setMonitoringContractUpperBreach(
                        (int) Math.round(Float.parseFloat(myTradeObject.getMonitoringContractEntryPrice()))
                        - Math.round(Math.abs(2 * Float.parseFloat(myTradeObject.getMonitoringContractEntryStdDev())))
                );
            }            
        } else {
            if (myTradeObject.getSideAndSize() > 0) {
                myTradeObject.setMonitoringContractLowerBreach(
                        Math.round(Float.parseFloat(myTradeObject.getMonitoringContractEntryPrice()))
                        - INITIALSTOPLOSSAMOUNT
                );
                myTradeObject.setMonitoringContractUpperBreach(
                        (int) Math.round(Float.parseFloat(myTradeObject.getMonitoringContractEntryPrice()))
                        + Math.round(Math.abs(2 * Float.parseFloat(myTradeObject.getMonitoringContractEntryStdDev())))
                );
            } else if (myTradeObject.getSideAndSize() < 0) {
                myTradeObject.setMonitoringContractLowerBreach(
                        Math.round(Float.parseFloat(myTradeObject.getMonitoringContractEntryPrice()))
                        + INITIALSTOPLOSSAMOUNT
                );
                myTradeObject.setMonitoringContractUpperBreach(
                        (int) Math.round(Float.parseFloat(myTradeObject.getMonitoringContractEntryPrice()))
                        - Math.round(Math.abs(2 * Float.parseFloat(myTradeObject.getMonitoringContractEntryStdDev())))
                );
            }            
        }

        if (bidAskPriceDetails.length() > 0) {
            myTradeObject.setTradingContractEntryBidAskFillDetails(bidAskPriceDetails);
        }

        myTradeObject.setOrderState(orderStatus);
        myTradeObject.setExpiry(futExpiry);
        myTradeObject.setTradingContractEntryOrderIDs(entryOrderId);
        myTradeObject.setTradingContractLastKnownSpread(tradingContractSpread);
        myTradeObject.setTradingContractLastUpdatedTimeStamp("-1");
        myTradeObject.setMonitoringContractLastKnownPrice(monitoringContractPrice);
        myTradeObject.setMonitoringContractLastUpdatedTimeStamp("-1");

        // Update the open positions key with entry Signal
        Jedis jedis = jedisPool.getResource();
        jedis.hset(queueKeyName, Integer.toString(slotNumber), myTradeObject.getCompleteTradingObjectString());
        jedisPool.returnResource(jedis);

    }

    int myPlaceConfiguredOrder(String symbolName, int quantity, String mktAction) {

        int returnOrderId = 0;
        // Possible order types are as follows
        // market, relativewithzeroaslimitwithamountoffset, relativewithmidpointaslimitwithamountoffset, relativewithzeroaslimitwithpercentoffset, relativewithmidpointaslimitwithpercentoffset 
        System.out.println(String.format("%1$tY%1$tm%1$td:%1$tH:%1$tM:%1$tS ", Calendar.getInstance(exchangeTimeZone)) + "Placing following Order - Symbol :" + symbolName + " quantity " + quantity + " expiry " + futExpiry + " mktAction " + mktAction + " orderType " + orderTypeToUse);

        if (orderTypeToUse.equalsIgnoreCase("market")) {
            if (contractTypeToUse.equalsIgnoreCase("STK")) {
                // Place order for STK type
                returnOrderId = ibInteractionClient.placeStkOrderAtMarket(symbolName, quantity, mktAction, strategyName, true);
            } else if (contractTypeToUse.equalsIgnoreCase("FUT")) {
                // Place Order for FUT type
                returnOrderId = ibInteractionClient.placeFutOrderAtMarket(symbolName, quantity, futExpiry, mktAction, strategyName, true);
            } else if (contractTypeToUse.equalsIgnoreCase("OPT")) {
                // Place Order for OPT type
                if (rightTypeToUse.equalsIgnoreCase("CALL") || rightTypeToUse.equalsIgnoreCase("C")) {
                    returnOrderId = ibInteractionClient.placeCallOptionOrderAtMarket(symbolName, quantity, futExpiry, strikePriceToUse, mktAction, strategyName, true);
                } else if (rightTypeToUse.equalsIgnoreCase("PUT") || rightTypeToUse.equalsIgnoreCase("P")) {
                    returnOrderId = ibInteractionClient.placePutOptionOrderAtMarket(symbolName, quantity, futExpiry, strikePriceToUse, mktAction, strategyName, true);
                }
            }
        } else if (orderTypeToUse.equalsIgnoreCase("relativewithzeroaslimitwithamountoffset")) {
            double limitPrice = 0.0; // For relative order, Limit price is suggested to be left as zero
            double offsetAmount = 0.0; // zero means it will take default value based on exchange / timezone
            if (contractTypeToUse.equalsIgnoreCase("STK")) {
                // Place order for STK type
                returnOrderId = ibInteractionClient.placeStkOrderAtRelative(symbolName, quantity, mktAction, strategyName, limitPrice, offsetAmount, true);
            } else if (contractTypeToUse.equalsIgnoreCase("FUT")) {
                // Place Order for FUT type
                returnOrderId = ibInteractionClient.placeFutOrderAtRelative(symbolName, quantity, futExpiry, mktAction, strategyName, limitPrice, offsetAmount, true);
            } else if (contractTypeToUse.equalsIgnoreCase("OPT")) {
                // Place Order for OPT type
                if (rightTypeToUse.equalsIgnoreCase("CALL") || rightTypeToUse.equalsIgnoreCase("C")) {
                    // for Options Relative to Market is not supported
                    returnOrderId = ibInteractionClient.placeCallOptionOrderAtMarket(symbolName, quantity, futExpiry, strikePriceToUse, mktAction, strategyName, true);
                } else if (rightTypeToUse.equalsIgnoreCase("PUT") || rightTypeToUse.equalsIgnoreCase("P")) {
                    // for Options Relative to Market is not supported                    
                    returnOrderId = ibInteractionClient.placePutOptionOrderAtMarket(symbolName, quantity, futExpiry, strikePriceToUse, mktAction, strategyName, true);
                }
            }
        }

        return (returnOrderId);
    }
    
    double getMonitoringContractPrice() {
        
        double monitoringContractPrice = 0.0;
        int reqId4MonitoringContract = -1;

        // check for subscription
        if (monitoringContractTypeToUse.equalsIgnoreCase("IND")) {
            // for IND type
            reqId4MonitoringContract = ibInteractionClient.requestIndMktDataSubscription(legName);                
        } else if (monitoringContractTypeToUse.equalsIgnoreCase("STK")) {
            // for STK type
            reqId4MonitoringContract = ibInteractionClient.requestStkMktDataSubscription(legName);                
        } else if (monitoringContractTypeToUse.equalsIgnoreCase("FUT")) {
            // for FUT type
            reqId4MonitoringContract = ibInteractionClient.requestFutMktDataSubscription(legName, futExpiry);                
        } else if (monitoringContractTypeToUse.equalsIgnoreCase("OPT")) {
            // for FUT type
            reqId4MonitoringContract = ibInteractionClient.requestOptMktDataSubscription(legName, futExpiry, rightTypeToUse, strikePriceToUse);                
        }         
                
        if (ibInteractionClient.myTickDetails.containsKey(reqId4MonitoringContract)) {
            monitoringContractPrice = ibInteractionClient.myTickDetails.get(reqId4MonitoringContract).getSymbolLastPrice();
        }    
        
        return(monitoringContractPrice);
    }
    
    int requestTickDataSnapshotForTradingContract() {

        int reqId4TickSnapshotRequest = 0;
        // Place Order and get the order ID
        if (contractTypeToUse.equalsIgnoreCase("STK")) {
            // for STK type
            reqId4TickSnapshotRequest = ibInteractionClient.reqTickDataSnapshotForStk(legName);
        } else if (contractTypeToUse.equalsIgnoreCase("FUT")) {
            // for FUT type
            reqId4TickSnapshotRequest = ibInteractionClient.reqTickDataSnapshotForFut(legName, futExpiry);
        } else if (contractTypeToUse.equalsIgnoreCase("OPT")) {
            // for OPT type
            if (rightTypeToUse.equalsIgnoreCase("CALL") || rightTypeToUse.equalsIgnoreCase("C")) {
                reqId4TickSnapshotRequest = ibInteractionClient.reqTickDataSnapshotForCallOption(legName, futExpiry, strikePriceToUse);
            } else if (rightTypeToUse.equalsIgnoreCase("PUT") || rightTypeToUse.equalsIgnoreCase("P")) {
                reqId4TickSnapshotRequest = ibInteractionClient.reqTickDataSnapshotForPutOption(legName, futExpiry, strikePriceToUse);
            }            
        }
        return(reqId4TickSnapshotRequest);
    }
  
    boolean enterLegPosition() {
      
        boolean orderFillStatus = false;
        System.out.println(String.format("%1$tY%1$tm%1$td:%1$tH:%1$tM:%1$tS ", Calendar.getInstance(exchangeTimeZone)) + "Blocked Slot Number " + slotNumber + " for entering position as - ");
        System.out.println(String.format("%1$tY%1$tm%1$td:%1$tH:%1$tM:%1$tS ", Calendar.getInstance(exchangeTimeZone)) + slotNumber + " : Symbol " + legName + " quantity " + legLotSize + " expiry " + futExpiry + " orderType " + orderTypeToUse + " right " + rightTypeToUse + " strike " + strikePriceToUse);

        double legSpread = 0.0;
        double legMonitoringContractPrice = this.getMonitoringContractPrice();
        int reqIdTickSnapshot = this.requestTickDataSnapshotForTradingContract();
        
        int legOrderId = this.myPlaceConfiguredOrder(legName, legLotSize, legPosition);
        legMonitoringContractPrice = this.getMonitoringContractPrice();

        System.out.println(String.format("%1$tY%1$tm%1$td:%1$tH:%1$tM:%1$tS ", Calendar.getInstance(exchangeTimeZone)) + "Placed Orders - with orderID as : " + legOrderId + " for " + legPosition);
        String bidAskDetails = legName + "_" + ibInteractionClient.myTickDetails.get(reqIdTickSnapshot).getSymbolBidPrice() + "_" + ibInteractionClient.myTickDetails.get(reqIdTickSnapshot).getSymbolAskPrice();
        // update the Open position queue with order inititated status message
        updateOpenPositionsQueue(openPositionsQueueKeyName, legDetails, "entryorderinitiated", legSpread, legOrderId, slotNumber, bidAskDetails, legMonitoringContractPrice);

        if (legOrderId > 0) {
            // Wait for orders to be completely filled            
            if (entryOrderCompletelyFilled(legOrderId, 750)) {
                bidAskDetails = legName + "_" + ibInteractionClient.myTickDetails.get(reqIdTickSnapshot).getSymbolBidPrice() + "_" + ibInteractionClient.myTickDetails.get(reqIdTickSnapshot).getSymbolAskPrice();
                bidAskDetails = bidAskDetails + "__" + legOrderId + "_" + legName + "_" + ibInteractionClient.myOrderStatusDetails.get(legOrderId).getFilledPrice();
                if (ibInteractionClient.myTickDetails.get(reqIdTickSnapshot).getContractDet().m_secType.equalsIgnoreCase("OPT")) {
                    bidAskDetails = bidAskDetails + "_OPTIONGREEKS" +
                            "_delta_" + String.format("%.4f", ibInteractionClient.myTickDetails.get(reqIdTickSnapshot).getOptionDelta()) +
                            "_gamma_" + String.format("%.5f", ibInteractionClient.myTickDetails.get(reqIdTickSnapshot).getOptionGamma()) +
                            "_impVol_" + String.format("%.5f", ibInteractionClient.myTickDetails.get(reqIdTickSnapshot).getOptionImpliedVolatilityAtLastPrice()) +
                            "_vega_" + String.format("%.3f", ibInteractionClient.myTickDetails.get(reqIdTickSnapshot).getOptionVega()) +
                            "_theta_" + String.format("%.3f", ibInteractionClient.myTickDetails.get(reqIdTickSnapshot).getOptionTheta()) + 
                            "_optPrice_" + String.format("%.3f", ibInteractionClient.myTickDetails.get(reqIdTickSnapshot).getOptionPrice()) + 
                            "_underlyingPrice_" + String.format("%.3f", ibInteractionClient.myTickDetails.get(reqIdTickSnapshot).getOptionUnderlyingPrice())
                            ;                    
                }                
                System.out.println(String.format("%1$tY%1$tm%1$td:%1$tH:%1$tM:%1$tS ", Calendar.getInstance(exchangeTimeZone)) + "Entry Order leg filled for  Order id " + legOrderId + " order side " + legPosition + " at avg filled price " + ibInteractionClient.myOrderStatusDetails.get(legOrderId).getFilledPrice());
                legSpread = ibInteractionClient.myOrderStatusDetails.get(legOrderId).getFilledPrice() * legLotSize;
                // update Redis queue with entered order
                updateOpenPositionsQueue(openPositionsQueueKeyName, legDetails, "entryorderfilled", legSpread, legOrderId, slotNumber, bidAskDetails, legMonitoringContractPrice);
            } else {
                int requestId = ibInteractionClient.requestExecutionDetailsHistorical(1);
                // wait till details are received OR for timeout to happen
                int timeOut = 0;
                while ((timeOut < 31)
                        && (!(ibInteractionClient.requestsCompletionStatus.get(requestId)) ) ) {
                    myUtils.waitForNSeconds(5);
                    timeOut = timeOut + 5;
                }
                bidAskDetails = legName + "_" + ibInteractionClient.myTickDetails.get(reqIdTickSnapshot).getSymbolBidPrice() + "_" + ibInteractionClient.myTickDetails.get(reqIdTickSnapshot).getSymbolAskPrice();
                bidAskDetails = bidAskDetails + "__" + legOrderId + "_" + legName + "_" + ibInteractionClient.myOrderStatusDetails.get(legOrderId).getFilledPrice();
                System.out.println(String.format("%1$tY%1$tm%1$td:%1$tH:%1$tM:%1$tS ", Calendar.getInstance(exchangeTimeZone)) + "Entry Order leg filled for  Order id " + legOrderId + " order side " + legPosition + " at avg filled price " + ibInteractionClient.myOrderStatusDetails.get(legOrderId).getFilledPrice());
                if (ibInteractionClient.myTickDetails.get(reqIdTickSnapshot).getContractDet().m_secType.equalsIgnoreCase("OPT")) {
                    bidAskDetails = bidAskDetails + "_OPTIONGREEKS" +
                            "_delta_" + String.format("%.4f", ibInteractionClient.myTickDetails.get(reqIdTickSnapshot).getOptionDelta()) +
                            "_gamma_" + String.format("%.5f", ibInteractionClient.myTickDetails.get(reqIdTickSnapshot).getOptionGamma()) +
                            "_impVol_" + String.format("%.5f", ibInteractionClient.myTickDetails.get(reqIdTickSnapshot).getOptionImpliedVolatilityAtLastPrice()) +
                            "_vega_" + String.format("%.3f", ibInteractionClient.myTickDetails.get(reqIdTickSnapshot).getOptionVega()) +
                            "_theta_" + String.format("%.3f", ibInteractionClient.myTickDetails.get(reqIdTickSnapshot).getOptionTheta()) + 
                            "_optPrice_" + String.format("%.3f", ibInteractionClient.myTickDetails.get(reqIdTickSnapshot).getOptionPrice()) + 
                            "_underlyingPrice_" + String.format("%.3f", ibInteractionClient.myTickDetails.get(reqIdTickSnapshot).getOptionUnderlyingPrice()) ;                    
                }                
                legSpread = ibInteractionClient.myOrderStatusDetails.get(legOrderId).getFilledPrice() * legLotSize;
                // update Redis queue with entered order
                updateOpenPositionsQueue(openPositionsQueueKeyName, legDetails, "entryorderinitiated", legSpread, legOrderId, slotNumber, bidAskDetails, legMonitoringContractPrice);
                System.out.println(String.format("%1$tY%1$tm%1$td:%1$tH:%1$tM:%1$tS ", Calendar.getInstance(exchangeTimeZone)) + "Please Check Order Status manually as entry Order initiated but did not receive Confirmation for Orders filling for Order id " + legOrderId);
            }
        }
        
        if ((ibInteractionClient.myOrderStatusDetails.containsKey(legOrderId)) && (ibInteractionClient.myOrderStatusDetails.get(legOrderId).getRemainingQuantity() == 0)) {
            orderFillStatus = true;
        }           
               
        return(orderFillStatus);
    }

    @Override
    public void run() {

        this.setName(threadName);
        // Place market Order with IB. Place the order in same sequence as order id is generated. if orderid sent is less than any of previous order id then duplicate order id message would be received
        if (!ibInteractionClient.waitForConnection(60)) {
            // Debug Message
            System.out.println(String.format("%1$tY%1$tm%1$td:%1$tH:%1$tM:%1$tS ", Calendar.getInstance(exchangeTimeZone)) + "IB Connection not available. Can not Enter Position as " + legName + ". Exiting entry thread now.");
        } else {
            boolean orderFillStatus = enterLegPosition();       
            // Debug Message
            System.out.println(String.format("%1$tY%1$tm%1$td:%1$tH:%1$tM:%1$tS ", Calendar.getInstance(exchangeTimeZone)) + "Entered Position for " + legName + " in Slot Number " + slotNumber + " with order Fill Status " + orderFillStatus + ". Exiting Entry thread now.");
        }
    }

}
