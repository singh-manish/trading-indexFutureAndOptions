/*
 The MIT License (MIT)

 Copyright (c) 2016 Manish Kumar Singh

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
package tradingstkfutopt;

import com.ib.client.*;
import java.text.*;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.logging.*;
import redis.clients.jedis.*;

/**
 * @author Manish Kumar Singh
 */
public class IBInteraction implements EWrapper {

    // variables / constants declarations
    // variable to open socket connection 
    public EClientSocket ibClient = new EClientSocket(this);

    private String myIPAddress;
    private int myPortNum;
    private int myClientId;

    private JedisPool jedisPool;
    private String orderIDField = "INRIBPAPERORDERID";

    // variable for uitlities class
    private MyUtils myUtils;

    private Object lockOrderPlacement = new Object();

    public ConcurrentHashMap<Integer, Boolean> requestsCompletionStatus = new ConcurrentHashMap<>();    
    public ConcurrentHashMap<Integer, MyTickObjClass> myTickDetails = new ConcurrentHashMap<Integer, MyTickObjClass>();
    public ConcurrentHashMap<Integer, MyBidAskPriceObjClass> myBidAskPriceDetails = new ConcurrentHashMap<Integer, MyBidAskPriceObjClass>();
    public ConcurrentHashMap<Integer, MyOrderStatusObjClass> myOrderStatusDetails = new ConcurrentHashMap<Integer, MyOrderStatusObjClass>();

    private int debugLevel = 0;

    private int initialValidOrderID = -1;
    private int nextRequestId = 1;

    private MyExchangeClass myExchangeObj;
    private double defaultOffsetForRelativeOrder = 0.05;

    // Constructor to inititalize variables
    IBInteraction(JedisPool jedisConnectionPool, String orderIDIncrField, String ibAPIIPAddress, int ibAPIPortNumber, int ibAPIClientId, MyUtils utils, MyExchangeClass exchangeObj) {
        jedisPool = jedisConnectionPool;
        myIPAddress = ibAPIIPAddress;
        myPortNum = ibAPIPortNumber;
        myClientId = ibAPIClientId;
        orderIDField = orderIDIncrField;
        myUtils = utils;
        myExchangeObj = exchangeObj;
        nextRequestId = 1;

        TimeZone.setDefault(myExchangeObj.getExchangeTimeZone());

        if (myExchangeObj.getExchangeCurrency().equalsIgnoreCase("inr")) {
            defaultOffsetForRelativeOrder = 0.05;
        } else if (myExchangeObj.getExchangeCurrency().equalsIgnoreCase("usd")) {
            defaultOffsetForRelativeOrder = 0.01;
        }

    }

    // Custom functions
    public int getNextRequestId() {
        nextRequestId++;
        return(nextRequestId);
    }
    
    public boolean connectToIB(int timeout) {

        ibClient.eConnect(myIPAddress, myPortNum, myClientId);

        //ibClient.reqIds(1); // This may be required in case valide order IDs are not being returned/generated
        int waitTime = 0;
        myUtils.waitForNSeconds(2);
        while (initialValidOrderID < 0) {
            if (timeout == 0) {
                waitTime++;
                myUtils.waitForNSeconds(1);
            } else if (waitTime <= timeout) {
                waitTime++;
                myUtils.waitForNSeconds(1);
            }
        }

        if (initialValidOrderID > 0) {
            // Set the orderID in IB Client as next valid Order ID and keep incrementing it for all subsequent order
            myUtils.setNextOrderID(jedisPool, orderIDField, initialValidOrderID, true);
            return (true);
        } else {
            return (false);
        }
    } //end of connectToIB

    public void disconnectFromIB() {
        ibClient.eDisconnect();
    } // End of disconnectFromIB

    public boolean waitForConnection(int timeout) {

        int waitTime = 0;
        while (!ibClient.isConnected()) {
            if (timeout == 0) {
                waitTime++;
                myUtils.waitForNSeconds(1);
            } else if (waitTime <= timeout) {
                waitTime++;
                myUtils.waitForNSeconds(1);
            }
        }
        if (ibClient.isConnected()) {
            return (true);
        } else {
            return (false);
        }

    } // End of waitForConnection

    int getBidAskPriceForStk(String symbol) {

        Contract myContract = new Contract();
        myContract.m_symbol = symbol;
        myContract.m_secType = "STK";
        myContract.m_exchange = myExchangeObj.getExchangeName();
        myContract.m_currency = myExchangeObj.getExchangeCurrency();

        int requestId = this.getNextRequestId();        
        myBidAskPriceDetails.put(requestId, new MyBidAskPriceObjClass(requestId));
        myBidAskPriceDetails.get(requestId).setRequestId(requestId);

        ibClient.reqMktData(requestId, myContract, "", true);
        requestsCompletionStatus.put(requestId, Boolean.FALSE);   
        
        return(requestId);
        
    } // End of getBidAskPriceForStk    

    void stopGettingBidAskPriceForStk(int requestID) {

        ibClient.cancelMktData(requestID);

    } // End of stopGettingBidAskPriceForStk() 

    boolean checkIndMktDataSubscription(String symbol) {
        // check if existing subscription exists for given symbol
        // if exists then return true
        // if subscription does not exist then return false
        boolean subscriptionStatus = false;
        for (int key : myTickDetails.keySet()) {
            if ((myTickDetails.get(key).getContractDet().m_symbol.equalsIgnoreCase(symbol))
                    && (myTickDetails.get(key).getContractDet().m_secType.equals("IND"))
                    && (myTickDetails.get(key).getSubscriptionStatus())) {
                subscriptionStatus = true;
            }
        }
        return (subscriptionStatus);
    } // End of checkIndMktDataSubscription  
    
    int requestIndMktDataSubscription(String symbol) {

        int returnRequestId = 0;
        // check if existing subscription exists for given symbol
        // if exists then return corresponding requestId.
        // if subscription does not exist then request one and return true
        boolean subscriptionExists = false;
        for (int key : myTickDetails.keySet()) {
            if ((myTickDetails.get(key).getContractDet().m_symbol.equalsIgnoreCase(symbol))
                    && (myTickDetails.get(key).getContractDet().m_secType.equals("IND"))
                    && (myTickDetails.get(key).getSubscriptionStatus())) {
                subscriptionExists = true;
                returnRequestId = key;
            }
        }

        if (!(subscriptionExists)) {
            // subscription does not exist so request one
            int requestId = this.getNextRequestId();
            myTickDetails.put(requestId, new MyTickObjClass(requestId));
            myTickDetails.get(requestId).setRequestId(requestId);
            //(String symbol, String currency, String securityType, String exchange, String expiry)            
            myTickDetails.get(requestId).setContractDetInd(symbol, myExchangeObj.getExchangeCurrency(), myExchangeObj.getExchangeName());
            myTickDetails.get(requestId).setSubscriptionStatus(true);
            ibClient.reqMktData(requestId, myTickDetails.get(requestId).getContractDet(), "", false);
            returnRequestId = requestId;
        }
        return (returnRequestId);
    } // End of requestIndMktDataSubscription    
    
    boolean checkStkMktDataSubscription(String symbol) {
        // check if existing subscription exists for given symbol
        // if exists then return true
        // if subscription does not exist then return false
        boolean subscriptionStatus = false;
        for (int key : myTickDetails.keySet()) {
            if ((myTickDetails.get(key).getContractDet().m_symbol.equalsIgnoreCase(symbol))
                    && (myTickDetails.get(key).getContractDet().m_secType.equals("STK"))
                    && (myTickDetails.get(key).getSubscriptionStatus())) {
                subscriptionStatus = true;
            }
        }
        return (subscriptionStatus);
    } // End of checkStkMktDataSubscription  
    
    int requestStkMktDataSubscription(String symbol) {

        int returnRequestId = 0;
        // check if existing subscription exists for given symbol
        // if exists then return corresponding requestId.
        // if subscription does not exist then request one and return true
        boolean subscriptionExists = false;
        for (int key : myTickDetails.keySet()) {
            if ((myTickDetails.get(key).getContractDet().m_symbol.equalsIgnoreCase(symbol))
                    && (myTickDetails.get(key).getContractDet().m_secType.equals("STK"))
                    && (myTickDetails.get(key).getSubscriptionStatus())) {
                subscriptionExists = true;
                returnRequestId = key;
            }
        }

        if (!(subscriptionExists)) {
            // subscription does not exist so request one
            int requestId = this.getNextRequestId();            
            myTickDetails.put(requestId, new MyTickObjClass(requestId));
            myTickDetails.get(requestId).setRequestId(requestId);
            //(String symbol, String currency, String securityType, String exchange, String expiry)            
            myTickDetails.get(requestId).setContractDetStk(symbol, myExchangeObj.getExchangeCurrency(), myExchangeObj.getExchangeName());
            myTickDetails.get(requestId).setSubscriptionStatus(true);
            ibClient.reqMktData(requestId, myTickDetails.get(requestId).getContractDet(), "", false);
            returnRequestId = requestId;
        }
        return (returnRequestId);
    } // End of requestStkMktDataSubscription    

    int getBidAskPriceForFut(String symbol, String expiry) {

        Contract myContract = new Contract();
        myContract.m_symbol = symbol;
        myContract.m_secType = "FUT";
        myContract.m_exchange = myExchangeObj.getExchangeName();
        myContract.m_currency = myExchangeObj.getExchangeCurrency();
        myContract.m_expiry = expiry;
        
        int requestId = this.getNextRequestId();        
        myBidAskPriceDetails.put(requestId, new MyBidAskPriceObjClass(requestId));
        myBidAskPriceDetails.get(requestId).setRequestId(requestId);

        ibClient.reqMktData(requestId, myContract, "", true);
        requestsCompletionStatus.put(requestId, Boolean.FALSE);           
        
        return(requestId);

    } // End of getBidAskPriceForFut    

    void stopGettingBidAskPriceForFut(int requestID) {

        ibClient.cancelMktData(requestID);

    } // End of stopGettingBidAskPriceForFut() 
    
    boolean checkFutMktDataSubscription(String symbol, String expiry) {

        // check if existing subscription exists for given symbol
        // if exists then return true
        // if subscription does not exist then return false
        boolean subscriptionStatus = false;
        for (int key : myTickDetails.keySet()) {
            if ((myTickDetails.get(key).getContractDet().m_symbol.equalsIgnoreCase(symbol))
                    && (myTickDetails.get(key).getContractDet().m_secType.equals("FUT"))
                    && (myTickDetails.get(key).getContractDet().m_expiry.equals(expiry))
                    && (myTickDetails.get(key).getSubscriptionStatus())) {
                subscriptionStatus = true;
            }
        }
        return (subscriptionStatus);
    } // End of checkFutMktDataSubscription    
    
    int requestFutMktDataSubscription(String symbol, String expiry) {

        int returnRequestId = 0;
        // check if existing subscription exists for given symbol
        // if exists then return request ID of subscription.
        // if subscription does not exist then request one and return request ID.
        boolean subscriptionExists = false;
        for (int key : myTickDetails.keySet()) {
            if ((myTickDetails.get(key).getContractDet().m_symbol.equalsIgnoreCase(symbol))
                    && (myTickDetails.get(key).getContractDet().m_secType.equals("FUT"))
                    && (myTickDetails.get(key).getContractDet().m_expiry.equals(expiry))
                    && (myTickDetails.get(key).getSubscriptionStatus())) {
                subscriptionExists = true;
                returnRequestId = key;
            }
        }

        if (!(subscriptionExists)) {
            // subscription does not exist so request one
            int requestId = this.getNextRequestId();            
            myTickDetails.put(requestId, new MyTickObjClass(requestId));
            myTickDetails.get(requestId).setRequestId(requestId);
            //(String symbol, String currency, String securityType, String exchange, String expiry)            
            myTickDetails.get(requestId).setContractDetFut(symbol, myExchangeObj.getExchangeCurrency(), myExchangeObj.getExchangeName(), expiry);
            myTickDetails.get(requestId).setSubscriptionStatus(true);
            ibClient.reqMktData(requestId, myTickDetails.get(requestId).getContractDet(), "", false);
            returnRequestId = requestId;
        }
        return (returnRequestId);
    } // End of requestFutMktDataSubscription    

    int getBidAskPriceForCallOption(String symbol, String expiry, double strikePrice) {
        
        String rightType = "CALL";
        return(getBidAskPriceForOpt(symbol, expiry, rightType, strikePrice));
    }
    
    int getBidAskPriceForPutOption(String symbol, String expiry, double strikePrice) {
        
        String rightType = "PUT";
        return(getBidAskPriceForOpt(symbol, expiry, rightType, strikePrice));        
    }
        
    int getBidAskPriceForOpt(String symbol, String expiry, String rightType, double strikePrice) {

        Contract myContract = new Contract();
        myContract.m_symbol = symbol;
        myContract.m_secType = "OPT";
        myContract.m_exchange = myExchangeObj.getExchangeName();
        myContract.m_currency = myExchangeObj.getExchangeCurrency();
        myContract.m_expiry = expiry;
        // Options Specific
        myContract.m_right = rightType;
        myContract.m_strike = strikePrice;        

        int requestId = this.getNextRequestId();                
        myBidAskPriceDetails.put(requestId, new MyBidAskPriceObjClass(requestId));
        myBidAskPriceDetails.get(requestId).setRequestId(requestId);

        ibClient.reqMktData(requestId, myContract, "", true);
        requestsCompletionStatus.put(requestId, Boolean.FALSE);
        
        return(requestId);

    } // End of getBidAskPriceForOpt    

    void stopGettingBidAskPriceForOpt(int requestID) {

        ibClient.cancelMktData(requestID);

    } // End of stopGettingBidAskPriceForOpt 

    boolean checkCallOptionMktDataSubscription(String symbol, String expiry, double strikePrice) {

        String rightType = "CALL";
        boolean subscriptionStatus = false;
        subscriptionStatus = checkOptMktDataSubscription(symbol, expiry, rightType, strikePrice);
        
        return(subscriptionStatus);
    }
    
    boolean checkPutOptionMktDataSubscription(String symbol, String expiry, double strikePrice) {

        String rightType = "PUT";
        boolean subscriptionStatus = false;
        subscriptionStatus = checkOptMktDataSubscription(symbol, expiry, rightType, strikePrice);
        
        return(subscriptionStatus);        
    }
        
    boolean checkOptMktDataSubscription(String symbol, String expiry, String rightType, double strikePrice) {

        // check if existing subscription exists for given symbol
        // if exists then return true
        // if subscription does not exist then return false
        String rights_01 = "C";
        String rights_02 = "CALL";        
        if (rightType.equalsIgnoreCase("C") || rightType.equalsIgnoreCase("CALL")) {
            rights_01 = "C";
            rights_02 = "CALL";                    
        } else if (rightType.equalsIgnoreCase("P") || rightType.equalsIgnoreCase("PUT")) {
            rights_01 = "P";
            rights_02 = "PUT";            
        }
        boolean subscriptionStatus = false;
        for (int key : myTickDetails.keySet()) {
            if ((myTickDetails.get(key).getContractDet().m_symbol.equalsIgnoreCase(symbol))
                    && (myTickDetails.get(key).getContractDet().m_secType.equals("OPT"))
                    && (myTickDetails.get(key).getContractDet().m_right.equals(rights_01) || myTickDetails.get(key).getContractDet().m_right.equals(rights_02))
                    && (myTickDetails.get(key).getContractDet().m_strike == strikePrice )                    
                    && (myTickDetails.get(key).getContractDet().m_expiry.equals(expiry))
                    && (myTickDetails.get(key).getSubscriptionStatus())) {
                subscriptionStatus = true;
            }
        }
        return (subscriptionStatus);
    } // End of checkFutMktDataSubscription    
    
    int requestOptMktDataSubscription(String symbol, String expiry, String rightType, double strikePrice) {

        int returnRequestId = 0;
        // check if existing subscription exists for given symbol
        // if exists then return request ID of subscription.
        // if subscription does not exist then request one and return request ID.
        String rights_01 = "C";
        String rights_02 = "CALL";        
        if (rightType.equalsIgnoreCase("C") || rightType.equalsIgnoreCase("CALL")) {
            rights_01 = "C";
            rights_02 = "CALL";                    
        } else if (rightType.equalsIgnoreCase("P") || rightType.equalsIgnoreCase("PUT")) {
            rights_01 = "P";
            rights_02 = "PUT";            
        }
        
        boolean subscriptionExists = false;
        for (int key : myTickDetails.keySet()) {
            if ((myTickDetails.get(key).getContractDet().m_symbol.equalsIgnoreCase(symbol))
                    && (myTickDetails.get(key).getContractDet().m_secType.equals("OPT"))
                    && (myTickDetails.get(key).getContractDet().m_right.equals(rights_01) || myTickDetails.get(key).getContractDet().m_right.equals(rights_02))
                    && (myTickDetails.get(key).getContractDet().m_strike == strikePrice )                    
                    && (myTickDetails.get(key).getContractDet().m_expiry.equals(expiry))
                    && (myTickDetails.get(key).getSubscriptionStatus())) {
                subscriptionExists = true;
                returnRequestId = key;
            }
        }

        if (!(subscriptionExists)) {
            // subscription does not exist so request one
            int requestId = this.getNextRequestId();            
            myTickDetails.put(requestId, new MyTickObjClass(requestId));
            myTickDetails.get(requestId).setRequestId(requestId);
            //(String symbol, String currency, String securityType, String exchange, String expiry)            
            myTickDetails.get(requestId).setContractDetOpt(symbol, myExchangeObj.getExchangeCurrency(), myExchangeObj.getExchangeName(), expiry, rightType, strikePrice);
            myTickDetails.get(requestId).setSubscriptionStatus(true);
            ibClient.reqMktData(requestId, myTickDetails.get(requestId).getContractDet(), "", false);
            returnRequestId = requestId;
        }
        return (returnRequestId);
    } // End of requestFutMktDataSubscription    
    
    void cancelMktDataSubscription(int requestId) {

        ibClient.cancelMktData(requestId);
        if (myTickDetails.containsKey(requestId)) {
            myTickDetails.get(requestId).setSubscriptionStatus(false);
        }

    } // End of onCancelMktData()

    int requestExecutionDetailsHistorical(int numPrevDays) {

        Calendar startingTimeStamp = Calendar.getInstance(myExchangeObj.getExchangeTimeZone());
        startingTimeStamp.add(Calendar.DATE, -1 * numPrevDays);

        String startTime = String.format("%1$tY%1$tm%1$td-00:00:00", startingTimeStamp); // format is - yyyymmdd-hh:mm:ss

        ExecutionFilter myFilter = new ExecutionFilter();
        myFilter.m_exchange = myExchangeObj.getExchangeName();
        myFilter.m_time = startTime;

        int requestId = this.getNextRequestId();        
        if (requestId > 0) {
            System.out.println(String.format("%1$tY%1$tm%1$td:%1$tH:%1$tM:%1$tS ", Calendar.getInstance(myExchangeObj.getExchangeTimeZone())) + "requesting execution details for time after " + startTime);
        }

        requestsCompletionStatus.put(requestId, Boolean.FALSE);
        ibClient.reqExecutions(requestId, myFilter);
        
        return(requestId);

    } // end of requestExecutionDetailsHistorical

    int requestExecutionDetailsHistorical(int numPrevDays, String symbol) {

        Calendar startingTimeStamp = Calendar.getInstance(myExchangeObj.getExchangeTimeZone());
        startingTimeStamp.add(Calendar.DATE, -1 * numPrevDays);

        String startTime = String.format("%1$tY%1$tm%1$td-00:00:00", startingTimeStamp); // format is - yyyymmdd-hh:mm:ss

        ExecutionFilter myFilter = new ExecutionFilter();
        myFilter.m_clientId = myClientId;
        myFilter.m_exchange = myExchangeObj.getExchangeName();
        myFilter.m_time = startTime;
        myFilter.m_symbol = symbol;

        int requestId = this.getNextRequestId();        
        if (requestId > 0) {
            System.out.println(String.format("%1$tY%1$tm%1$td:%1$tH:%1$tM:%1$tS ", Calendar.getInstance(myExchangeObj.getExchangeTimeZone())) + "requesting execution details for time after " + startTime);
        }

        requestsCompletionStatus.put(requestId, Boolean.FALSE);        
        ibClient.reqExecutions(requestId, myFilter);
        
        return(requestId);

    } // end of requestExecutionDetailsHistorical
    
    public int placeStkOrderAtRelative(String symbol, int qty, String mktAction, String referenceComments, double limitPrice, double offsetAmount, boolean debugFlag) {

        int ibOrderId;
        Contract myContract = new Contract();
        Order myOrder = new Order();

        myContract.m_symbol = symbol;
        myContract.m_secType = "STK";
        myContract.m_exchange = myExchangeObj.getExchangeName();
        myContract.m_currency = myExchangeObj.getExchangeCurrency();

        myOrder.m_action = mktAction;
        myOrder.m_totalQuantity = qty;
        myOrder.m_orderType = "REL"; // At Relative to Market Price
        myOrder.m_lmtPrice = limitPrice;
        myOrder.m_auxPrice = defaultOffsetForRelativeOrder;
        if (offsetAmount > 0) {
            myOrder.m_auxPrice = offsetAmount;
        }
        myOrder.m_tif = "DAY"; // GTC - Good Till Cancel Order, DAY - Good Till Day
        myOrder.m_orderRef = referenceComments; // This is what gets displayed on TWS screen
        myOrder.m_transmit = true; // STP order i.e. transmit immediately
        synchronized (lockOrderPlacement) {
            ibOrderId = myUtils.getNextOrderID(jedisPool, orderIDField, debugFlag);
            ibClient.placeOrder(ibOrderId, myContract, myOrder);
            if (debugFlag) {
                System.out.println(String.format("%1$tY%1$tm%1$td:%1$tH:%1$tM:%1$tS ", Calendar.getInstance(myExchangeObj.getExchangeTimeZone())) + "Placed Relative Order for " + symbol + " for " + mktAction + " type STK " + " order ID " + ibOrderId + " limit " + limitPrice + " offsetAmt " + offsetAmount);
            }
        }

        return (ibOrderId);
    } // placeStkOrderAtRelative

    public int placeStkOrderAtMarket(String symbol, int qty, String mktAction, String referenceComments, boolean debugFlag) {

        int ibOrderId;
        Contract myContract = new Contract();
        Order myOrder = new Order();

        myContract.m_symbol = symbol;
        myContract.m_secType = "STK";
        myContract.m_exchange = myExchangeObj.getExchangeName();
        myContract.m_currency = myExchangeObj.getExchangeCurrency();

        myOrder.m_action = mktAction;
        myOrder.m_totalQuantity = qty;
        myOrder.m_orderType = "MKT"; // At Market Price
        //myOrder.m_allOrNone = true; // ALL or None are not supported in NSE
        myOrder.m_tif = "DAY"; // GTC - Good Till Cancel Order, DAY - Good Till Day
        myOrder.m_orderRef = referenceComments; // This is waht gets displayed on TWS screen
        myOrder.m_transmit = true; // STP order i.e. transmit immediately
        synchronized (lockOrderPlacement) {
            ibOrderId = myUtils.getNextOrderID(jedisPool, orderIDField, debugFlag);
            ibClient.placeOrder(ibOrderId, myContract, myOrder);

            if (debugFlag) {
                System.out.println(String.format("%1$tY%1$tm%1$td:%1$tH:%1$tM:%1$tS ", Calendar.getInstance(myExchangeObj.getExchangeTimeZone())) + "Placed Market Order for " + symbol + " for " + mktAction + " type STK " + " order ID " + ibOrderId);
            }
        }

        return (ibOrderId);
    } // placeStkOrderAtMarket

    public int placeFutOrderAtRelative(String symbol, int qty, String expiry, String mktAction, String referenceComments, double limitPrice, double offsetAmount, boolean debugFlag) {

        int ibOrderId;
        Contract myContract = new Contract();
        Order myOrder = new Order();

        myContract.m_symbol = symbol;
        myContract.m_secType = "FUT";
        myContract.m_exchange = myExchangeObj.getExchangeName();
        myContract.m_currency = myExchangeObj.getExchangeCurrency();
        myContract.m_expiry = expiry;

        myOrder.m_action = mktAction;
        myOrder.m_totalQuantity = qty;
        myOrder.m_orderType = "REL"; // At Market Price
        myOrder.m_lmtPrice = limitPrice;
        myOrder.m_auxPrice = defaultOffsetForRelativeOrder;
        if (offsetAmount > 0) {
            myOrder.m_auxPrice = offsetAmount;
        }
        myOrder.m_tif = "DAY"; // GTC - Good Till Cancel Order, DAY - Good Till Day
        myOrder.m_orderRef = referenceComments; // This is what gets displayed on TWS screen
        myOrder.m_transmit = true; // STP order i.e. transmit immediately
        synchronized (lockOrderPlacement) {
            ibOrderId = myUtils.getNextOrderID(jedisPool, orderIDField, debugFlag);
            ibClient.placeOrder(ibOrderId, myContract, myOrder);
            if (debugFlag) {
                System.out.println(String.format("%1$tY%1$tm%1$td:%1$tH:%1$tM:%1$tS ", Calendar.getInstance(myExchangeObj.getExchangeTimeZone())) + "Placed Relative Order for " + symbol + " for " + mktAction + " type FUT " + " expiry " + expiry + " order ID " + ibOrderId + " limit " + limitPrice + " offsetAmt " + offsetAmount);
            }
        }

        return (ibOrderId);
    } // placeFutOrderAtRelative

    public int placeFutOrderAtMarket(String symbol, int qty, String expiry, String mktAction, String referenceComments, boolean debugFlag) {

        int ibOrderId;
        Contract myContract = new Contract();
        Order myOrder = new Order();

        myContract.m_symbol = symbol;
        myContract.m_secType = "FUT";
        myContract.m_exchange = myExchangeObj.getExchangeName();
        myContract.m_currency = myExchangeObj.getExchangeCurrency();
        myContract.m_expiry = expiry;

        myOrder.m_action = mktAction;
        myOrder.m_totalQuantity = qty;
        myOrder.m_orderType = "MKT"; // At Market Price
        //myOrder.m_allOrNone = true; // ALL or None are not supported in NSE
        myOrder.m_tif = "DAY"; // GTC - Good Till Cancel Order, DAY - Good Till Day
        myOrder.m_orderRef = referenceComments; // This is waht gets displayed on TWS screen
        myOrder.m_transmit = true; // STP order i.e. transmit immediately
        synchronized (lockOrderPlacement) {
            ibOrderId = myUtils.getNextOrderID(jedisPool, orderIDField, debugFlag);
            ibClient.placeOrder(ibOrderId, myContract, myOrder);
            if (debugFlag) {
                System.out.println(String.format("%1$tY%1$tm%1$td:%1$tH:%1$tM:%1$tS ", Calendar.getInstance(myExchangeObj.getExchangeTimeZone())) + "Placed Market Order for " + symbol + " for " + mktAction + " type FUT " + " expiry " + expiry + " order ID " + ibOrderId);
            }
        }

        return (ibOrderId);
    } // placeFutOrderAtMarket
        
    public int placeCallOptionOrderAtMarket(String symbol, int qty, String expiry, double strikePrice, String mktAction, String referenceComments, boolean debugFlag) {

        String rightType = "CALL";
        int ibOrderId = 0;
        
        ibOrderId = placeOptOrderAtMarket(symbol, qty, expiry, rightType, strikePrice, mktAction, referenceComments, debugFlag);
        
        return(ibOrderId);
    }
    
    public int placePutOptionOrderAtMarket(String symbol, int qty, String expiry, double strikePrice, String mktAction, String referenceComments, boolean debugFlag) {

        String rightType = "PUT";
        int ibOrderId = 0;
        
        ibOrderId = placeOptOrderAtMarket(symbol, qty, expiry, rightType, strikePrice, mktAction, referenceComments, debugFlag);
        
        return(ibOrderId);
    }    

    public int placeOptOrderAtMarket(String symbol, int qty, String expiry, String rightType, double strikePrice, String mktAction, String referenceComments, boolean debugFlag) {

        int ibOrderId;
        Contract myContract = new Contract();
        Order myOrder = new Order();

        myContract.m_symbol = symbol;
        myContract.m_secType = "OPT";
        myContract.m_exchange = myExchangeObj.getExchangeName();
        myContract.m_currency = myExchangeObj.getExchangeCurrency();
        myContract.m_expiry = expiry;
        // Following are Option Related Fields
        myContract.m_right = rightType; // C or CALL or P or PUT
        myContract.m_strike = strikePrice;        

        myOrder.m_action = mktAction;
        myOrder.m_totalQuantity = qty;
        myOrder.m_orderType = "MKT"; // At Market Price
        //myOrder.m_allOrNone = true; // ALL or None are not supported in NSE
        myOrder.m_tif = "DAY"; // GTC - Good Till Cancel Order, DAY - Good Till Day
        myOrder.m_orderRef = referenceComments; // This is waht gets displayed on TWS screen
        myOrder.m_transmit = true; // STP order i.e. transmit immediately
        synchronized (lockOrderPlacement) {
            ibOrderId = myUtils.getNextOrderID(jedisPool, orderIDField, debugFlag);
            ibClient.placeOrder(ibOrderId, myContract, myOrder);
            if (debugFlag) {
                System.out.println(String.format("%1$tY%1$tm%1$td:%1$tH:%1$tM:%1$tS ", Calendar.getInstance(myExchangeObj.getExchangeTimeZone())) + "Placed Market Order for " + symbol + " for " + mktAction + " type OPT " + rightType + " for strike " + strikePrice + " expiry " + expiry + " order ID " + ibOrderId);
            }
        }

        return (ibOrderId);
    } // placeOptOrderAtMarket
    
    // overridden functions to receive data from IB interface / TWS
    @Override
    public void historicalData(int reqId, String date, double open, double high, double low,
            double close, int volume, int count, double WAP, boolean hasGaps) {

    } // End of historcialData(...)

    @Override
    public void tickPrice(int tickerId, int field, double price, int canAutoExecute) {

        if (myBidAskPriceDetails.containsKey(tickerId)) {
            if ((field == TickType.BID)) {
                myBidAskPriceDetails.get(tickerId).setSymbolBidPrice(price);
                myBidAskPriceDetails.get(tickerId).setBidPriceUpdateTime(System.currentTimeMillis());
                System.out.println("bidPrice " + price + " tickerId " + tickerId + " Time " + String.format("%1$tY%1$tm%1$td%1$tH%1$tM%1$tS", Calendar.getInstance(myExchangeObj.getExchangeTimeZone())));
            }
            if (field == TickType.ASK) {
                myBidAskPriceDetails.get(tickerId).setSymbolAskPrice(price);
                myBidAskPriceDetails.get(tickerId).setAskPriceUpdateTime(System.currentTimeMillis());
                System.out.println("askPrice " + price + " tickerId " + tickerId + " Time " + String.format("%1$tY%1$tm%1$td%1$tH%1$tM%1$tS", Calendar.getInstance(myExchangeObj.getExchangeTimeZone())));
            }
        }
        if (myTickDetails.containsKey(tickerId)) {
            if ((myTickDetails.get(tickerId).getSubscriptionStatus()) && (price > 0)) {
                if (field == TickType.CLOSE) {
                    myTickDetails.get(tickerId).setClosePriceUpdateTime(System.currentTimeMillis());
                    myTickDetails.get(tickerId).setSymbolClosePrice(price);
                }
                if (field == TickType.LAST) {
                    myTickDetails.get(tickerId).setLastPriceUpdateTime(System.currentTimeMillis());
                    myTickDetails.get(tickerId).setSymbolLastPrice(price);
                }
                if (field == TickType.BID) {
                    myTickDetails.get(tickerId).setBidPriceUpdateTime(System.currentTimeMillis());
                    myTickDetails.get(tickerId).setSymbolBidPrice(price);
                }
                if (field == TickType.ASK) {
                    myTickDetails.get(tickerId).setAskPriceUpdateTime(System.currentTimeMillis());
                    myTickDetails.get(tickerId).setSymbolAskPrice(price);
                }
            }
        }
    } // End of tickPrice(...)

    @Override
    public void tickSize(int tickerId, int field, int size) {

        if (myTickDetails.containsKey(tickerId)) {
            if (myTickDetails.get(tickerId).getSubscriptionStatus()) {
                if (field == TickType.VOLUME) {
                    myTickDetails.get(tickerId).setLastVolumeUpdateTime(System.currentTimeMillis());
                    myTickDetails.get(tickerId).setSymbolLastVolume(size);
                }
            }
        }
        if (myBidAskPriceDetails.containsKey(tickerId)) {
            if ((field == TickType.BID_SIZE)) {
                myBidAskPriceDetails.get(tickerId).setSymbolBidVolume(size);
                System.out.println("bidSize " + size + " tickerId " + tickerId + " Time " + String.format("%1$tY%1$tm%1$td%1$tH%1$tM%1$tS", Calendar.getInstance(myExchangeObj.getExchangeTimeZone())));
            }
            if (field == TickType.ASK_SIZE) {
                myBidAskPriceDetails.get(tickerId).setSymbolAskVolume(size);
                System.out.println("askSize " + size + " tickerId " + tickerId + " Time " + String.format("%1$tY%1$tm%1$td%1$tH%1$tM%1$tS", Calendar.getInstance(myExchangeObj.getExchangeTimeZone())));
            }
        }
    } // End of tickSize(...)

    @Override
    public void error(Exception e) {
        System.err.println(String.format("%1$tY%1$tm%1$td:%1$tH:%1$tM:%1$tS ", Calendar.getInstance()) + "IB interface exception occured as" + e.getMessage());
        //System.err.println(e.getMessage());
    }

    @Override
    public void error(String str) {
        System.err.println(String.format("%1$tY%1$tm%1$td:%1$tH:%1$tM:%1$tS ", Calendar.getInstance()) + "IB interface error occured as" + str);
        //System.err.println(str);
    }

    @Override
    public void error(int id, int errorCode, String errorMsg) {
        System.err.println(String.format("%1$tY%1$tm%1$td:%1$tH:%1$tM:%1$tS ", Calendar.getInstance()) + "IB interface error occured - error code : " + errorCode + " for reqId " + id + " error as " + errorMsg);
        //System.err.println(errorMsg);
    }

    @Override
    public void connectionClosed() {
    }

    public void tickOptionComputation(int tickerId, int field, double impliedVol,
            double delta, double modelPrice, double pvDividend) {
    }

    @Override
    public void tickGeneric(int tickerId, int tickType, double value) {
    }

    @Override
    public void tickString(int tickerId, int tickType, String value) {
    }

    @Override
    public void tickEFP(int tickerId, int tickType, double basisPoints,
            String formattedBasisPoints, double impliedFuture, int holdDays,
            String futureExpiry, double dividendImpact, double dividendsToExpiry) {
    }

    @Override
    public void orderStatus(int orderId, String status, int filled, int remaining,
            double avgFillPrice, int permId, int parentId, double lastFillPrice,
            int clientId, String whyHeld) {

        if (!(myOrderStatusDetails.containsKey(orderId))) {
            myOrderStatusDetails.put(orderId, new MyOrderStatusObjClass(orderId));
        }
        myOrderStatusDetails.get(orderId).setOrderId(orderId);
        myOrderStatusDetails.get(orderId).setFilledPrice(avgFillPrice);
        myOrderStatusDetails.get(orderId).setFilledQuantity(filled);
        myOrderStatusDetails.get(orderId).setRemainingQuantity(remaining);
        myOrderStatusDetails.get(orderId).setUpdateTime(System.currentTimeMillis());

        if (debugLevel > 4) {
            System.out.println(String.format("%1$tY%1$tm%1$td:%1$tH:%1$tM:%1$tS ", Calendar.getInstance(myExchangeObj.getExchangeTimeZone())) + "OrderId " + orderId + " status " + status + " filled qty " + filled + " remaining qty " + remaining + " average fill price " + avgFillPrice + " last filled price " + lastFillPrice);
        }

    }

    @Override
    public void openOrder(int orderId, Contract contract, Order order, OrderState orderState) {
    }

    @Override
    public void updateAccountValue(String key, String value, String currency, String accountName) {
    }

    @Override
    public void updatePortfolio(Contract contract, int position, double marketPrice, double marketValue,
            double averageCost, double unrealizedPNL, double realizedPNL, String accountName) {
    }

    @Override
    public void updateAccountTime(String timeStamp) {
    }

    @Override
    public void nextValidId(int orderId) {
        initialValidOrderID = orderId;
    }

    //public void contractDetails(ContractDetails contractDetails) {} gives reqid now, see below
    public void bondContractDetails(ContractDetails contractDetails) {
    }

    @Override
    public void execDetails(int orderId, Contract contract, Execution execution) {

        //System.out.println("reqId :" + orderId +" symbol :"+ contract.m_symbol + " expiry :" + contract.m_expiry + " execTime :" + execution.m_time + " avgPrice :" + execution.m_avgPrice + " execOrderId :" + execution.m_orderId + " price :" + execution.m_price + " qty :" + execution.m_cumQty + " numShares :" + execution.m_shares + " orderRef :" + execution.m_orderRef );
        if (!(myOrderStatusDetails.containsKey(execution.m_orderId))) {
            myOrderStatusDetails.put(execution.m_orderId, new MyOrderStatusObjClass(execution.m_orderId));
            myOrderStatusDetails.get(execution.m_orderId).setUpdateTime(System.currentTimeMillis());
        }
        myOrderStatusDetails.get(execution.m_orderId).setOrderId(orderId);
        myOrderStatusDetails.get(execution.m_orderId).setFilledPrice(execution.m_price);
        myOrderStatusDetails.get(execution.m_orderId).setAveragePrice(execution.m_avgPrice);        
        myOrderStatusDetails.get(execution.m_orderId).setFilledQuantity(execution.m_cumQty);
        myOrderStatusDetails.get(execution.m_orderId).setUniqueExecutionId(execution.m_execId);
        myOrderStatusDetails.get(execution.m_orderId).setOrderReference(execution.m_orderRef);        
        myOrderStatusDetails.get(execution.m_orderId).setRemainingQuantity(execution.m_cumQty - execution.m_shares);
        myOrderStatusDetails.get(execution.m_orderId).setContractDet(contract);       
        try {
            // Convert execution.m_time to long millisecond value yyyyMMddHHmmss
            Date tradeTime = new SimpleDateFormat("yyyyMMddHHmmss").parse(execution.m_time.replace(" ", "").replace(":", ""));
            myOrderStatusDetails.get(execution.m_orderId).setUpdateTime(tradeTime.getTime());
        } catch (ParseException ex) {
            Logger.getLogger(IBInteraction.class.getName()).log(Level.SEVERE, null, ex);
        }
    }

    @Override
    public void updateMktDepth(int tickerId, int position, int operation, int side, double price, int size) {
    }

    @Override
    public void updateMktDepthL2(int tickerId, int position, String marketMaker, int operation,
            int side, double price, int size) {
    }

    @Override
    public void updateNewsBulletin(int msgId, int msgType, String message, String origExchange) {
    }

    @Override
    public void managedAccounts(String accountsList) {
    }

    @Override
    public void receiveFA(int faDataType, String xml) {
    }

    @Override
    public void scannerParameters(String xml) {
    }

    @Override
    public void scannerData(int reqId, int rank, ContractDetails contractDetails, String distance,
            String benchmark, String projection, String legsStr) {
    }

    @Override
    public void scannerDataEnd(int reqId) {
    }

    @Override
    public void realtimeBar(int reqId, long time, double open, double high, double low, double close, long volume, double wap, int count) {
    }

    @Override
    public void currentTime(long time) {
    }

    @Override
    public void tickSnapshotEnd(int reqId) {
        requestsCompletionStatus.put(reqId, Boolean.TRUE);
    }

    @Override
    public void deltaNeutralValidation(int reqId, UnderComp underComp) {
    }

    @Override
    public void fundamentalData(int reqId, String data) {
    }

    @Override
    public void execDetailsEnd(int reqId) {
        requestsCompletionStatus.put(reqId, Boolean.TRUE);
    }

    @Override
    public void contractDetailsEnd(int reqId) {
    }

    @Override
    public void bondContractDetails(int reqId, ContractDetails contractDetails) {
    }

    @Override
    public void contractDetails(int reqId, ContractDetails contractDetails) {
    } //the new version

    @Override
    public void accountDownloadEnd(String accountName) {
    }

    @Override
    public void openOrderEnd() {
    }

    @Override
    public void tickOptionComputation(int tickerId, int field, double impliedVol, double delta, double optPrice, double pvDividend, double gamma, double vega, double theta, double undPrice) {
    }

    @Override
    public void marketDataType(int reqId, int marketDataType) {
    }

    @Override
    public void commissionReport(CommissionReport commissionReport) {        
        for (int orderId : myOrderStatusDetails.keySet()) {
            if (myOrderStatusDetails.get(orderId).getUniqueExecutionId().equalsIgnoreCase(commissionReport.m_execId)) {
                myOrderStatusDetails.get(orderId).setCommissionAmount(commissionReport.m_commission);
            }
        }
    }

    @Override
    public void position(String account, Contract contract, int pos, double avgCost) {
    }

    @Override
    public void positionEnd() {
    }

    @Override
    public void accountSummary(int reqId, String account, String tag, String value, String currency) {
    }

    @Override
    public void accountSummaryEnd(int reqId) {
    }
}
