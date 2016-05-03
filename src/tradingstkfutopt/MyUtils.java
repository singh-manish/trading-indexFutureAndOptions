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
package tradingstkfutopt;

import com.ib.client.Contract;
import java.text.*;
import java.util.*;
import java.util.logging.*;
import redis.clients.jedis.*;
import redis.clients.jedis.exceptions.JedisException;

/**
 * @author Manish Kumar Singh
 */

class MyExchangeClass {

    private TimeZone exchangeTimeZone;
    private int exchangeStartTimeHHMM, exchangeStartTimeHHMMSS, exchangeCloseTimeHHMM, exchangeCloseTimeHHMMSS;
    private int programStartTimeHHMM, programStartTimeHHMMSS, programCloseTimeHHMM, programCloseTimeHHMMSS;
    private String exchangeName, exchangeCurrency;

    public MyExchangeClass(TimeZone exTZ) {
        this.exchangeTimeZone = exTZ;
        if (exTZ.equals(TimeZone.getTimeZone("Asia/Calcutta"))) {
            this.exchangeStartTimeHHMM = 915;
            this.exchangeStartTimeHHMMSS = 91500;
            this.exchangeCloseTimeHHMM = 1530;
            this.exchangeCloseTimeHHMMSS = 153000;
            this.programStartTimeHHMM = 900;
            this.programStartTimeHHMMSS = 90000;
            this.programCloseTimeHHMM = 1535;
            this.programCloseTimeHHMMSS = 153500;
            this.exchangeName = "NSE";
            this.exchangeCurrency = "INR";
        } else if (exTZ.equals(TimeZone.getTimeZone("America/New_York"))) {
            this.exchangeStartTimeHHMM = 930;
            this.exchangeStartTimeHHMMSS = 93000;
            this.exchangeCloseTimeHHMM = 1600;
            this.exchangeCloseTimeHHMMSS = 160000;
            this.programStartTimeHHMM = 915;
            this.programStartTimeHHMMSS = 91500;
            this.programCloseTimeHHMM = 1605;
            this.programCloseTimeHHMMSS = 160500;
            this.exchangeName = "SMART";
            this.exchangeCurrency = "USD";
        }
    }

    public MyExchangeClass(String exchangeCurrency) {
        if (exchangeCurrency.equalsIgnoreCase("inr")) {
            this.exchangeTimeZone = TimeZone.getTimeZone("Asia/Calcutta");
            this.exchangeStartTimeHHMM = 915;
            this.exchangeStartTimeHHMMSS = 91500;
            this.exchangeCloseTimeHHMM = 1530;
            this.exchangeCloseTimeHHMMSS = 153000;
            this.programStartTimeHHMM = 900;
            this.programStartTimeHHMMSS = 90000;
            this.programCloseTimeHHMM = 1535;
            this.programCloseTimeHHMMSS = 153500;
            this.exchangeName = "NSE";
            this.exchangeCurrency = "INR";
        } else if (exchangeCurrency.equalsIgnoreCase("usd")) {
            this.exchangeTimeZone = TimeZone.getTimeZone("America/New_York");
            this.exchangeStartTimeHHMM = 930;
            this.exchangeStartTimeHHMMSS = 93000;
            this.exchangeCloseTimeHHMM = 1600;
            this.exchangeCloseTimeHHMMSS = 160000;
            this.programStartTimeHHMM = 915;
            this.programStartTimeHHMMSS = 91500;
            this.programCloseTimeHHMM = 1605;
            this.programCloseTimeHHMMSS = 160500;
            this.exchangeName = "SMART";
            this.exchangeCurrency = "USD";
        }
    }

    public TimeZone getExchangeTimeZone() {
        return this.exchangeTimeZone;
    }

    public int getExchangeStartTimeHHMM() {
        return this.exchangeStartTimeHHMM;
    }

    public String getStringExchangeStartTimeHHMM() {
        return String.format("%04d", this.exchangeStartTimeHHMM);
    }

    public int getExchangeStartTimeHHMMSS() {
        return this.exchangeStartTimeHHMMSS;
    }

    public String getStringExchangeStartTimeHHMMSS() {
        return String.format("%06d", this.exchangeStartTimeHHMMSS);
    }

    public int getExchangeCloseTimeHHMM() {
        return this.exchangeCloseTimeHHMM;
    }

    public String getStringExchangeCloseTimeHHMM() {
        return String.format("%04d", this.exchangeCloseTimeHHMM);
    }

    public int getExchangeCloseTimeHHMMSS() {
        return this.exchangeCloseTimeHHMMSS;
    }

    public String getStringExchangeCloseTimeHHMMSS() {
        return String.format("%06d", this.exchangeCloseTimeHHMMSS);
    }

    public int getProgramStartTimeHHMM() {
        return this.programStartTimeHHMM;
    }

    public String getStringProgramStartTimeHHMM() {
        return String.format("%04d", this.programStartTimeHHMM);
    }

    public int getProgramStartTimeHHMMSS() {
        return this.programStartTimeHHMMSS;
    }

    public String getStringProgramStartTimeHHMMSS() {
        return String.format("%06d", this.programStartTimeHHMMSS);
    }

    public int getProgramCloseTimeHHMM() {
        return this.programCloseTimeHHMM;
    }

    public String getStringProgramCloseTimeHHMM() {
        return String.format("%04d", this.programCloseTimeHHMM);
    }

    public int getProgramCloseTimeHHMMSS() {
        return this.programCloseTimeHHMMSS;
    }

    public String getStringProgramCloseTimeHHMMSS() {
        return String.format("%04d", this.programCloseTimeHHMMSS);
    }

    public String getExchangeName() {
        return this.exchangeName;
    }

    public String getExchangeCurrency() {
        return this.exchangeCurrency;
    }

}

// Define class to store last updated prices, and time of it
class MyTickObjClass {

    private int requestId, symbolLastVolume;
    private double symbolLastPrice, symbolClosePrice, symbolBidPrice, symbolAskPrice;
    private long lastVolumeUpdateTime, lastPriceUpdateTime, closePriceUpdateTime, bidPriceUpdateTime, askPriceUpdateTime;
    private boolean subscriptionStatus;
    private Contract contractDet = new Contract();

    public MyTickObjClass(int requestId) {
        this.requestId = requestId;
        this.symbolLastVolume = 0;
        this.symbolLastPrice = 0.0;
        this.symbolClosePrice = 0.0;
        this.symbolBidPrice = 0.0;
        this.symbolAskPrice = 0.0;
        this.lastVolumeUpdateTime = -1;
        this.lastPriceUpdateTime = -1;
        this.closePriceUpdateTime = -1;
        this.bidPriceUpdateTime = -1;
        this.askPriceUpdateTime = -1;
        this.subscriptionStatus = false;
    }

    public int getRequestId() {
        return this.requestId;
    }

    public int getSymbolLastVolume() {
        return this.symbolLastVolume;
    }

    public double getSymbolLastPrice() {
        return this.symbolLastPrice;
    }

    public double getSymbolClosePrice() {
        return this.symbolClosePrice;
    }

    public double getSymbolBidPrice() {
        return this.symbolBidPrice;
    }

    public double getSymbolAskPrice() {
        return this.symbolAskPrice;
    }

    public long getLastVolumeUpdateTime() {
        return this.lastVolumeUpdateTime;
    }

    public long getLastPriceUpdateTime() {
        return this.lastPriceUpdateTime;
    }

    public long getClosePriceUpdateTime() {
        return this.closePriceUpdateTime;
    }

    public long getBidPriceUpdateTime() {
        return this.bidPriceUpdateTime;
    }

    public long getAskPriceUpdateTime() {
        return this.askPriceUpdateTime;
    }

    public boolean getSubscriptionStatus() {
        return this.subscriptionStatus;
    }

    public Contract getContractDet() {
        return this.contractDet;
    }

    public String getSecurityType() {
        return this.contractDet.m_secType;
    }

    public void setRequestId(int requestId) {
        this.requestId = requestId;
    }

    public void setSymbolLastVolume(int volume) {
        this.symbolLastVolume = volume;
    }

    public void setSymbolLastPrice(double price) {
        this.symbolLastPrice = price;
    }

    public void setSymbolClosePrice(double price) {
        this.symbolClosePrice = price;
    }

    public void setSymbolBidPrice(double price) {
        this.symbolBidPrice = price;
    }

    public void setSymbolAskPrice(double price) {
        this.symbolAskPrice = price;
    }

    public void setLastVolumeUpdateTime(long time) {
        this.lastVolumeUpdateTime = time;
    }

    public void setLastPriceUpdateTime(long time) {
        this.lastPriceUpdateTime = time;
    }

    public void setClosePriceUpdateTime(long time) {
        this.closePriceUpdateTime = time;
    }

    public void setBidPriceUpdateTime(long time) {
        this.bidPriceUpdateTime = time;
    }

    public void setAskPriceUpdateTime(long time) {
        this.askPriceUpdateTime = time;
    }

    public void setSubscriptionStatus(boolean subscriptionStatus) {
        this.subscriptionStatus = subscriptionStatus;
    }

    // Constructor for contract Type STK
    private void setContractDet(String symbol, String currency, String securityType, String exchange) {
        this.contractDet.m_symbol = symbol;
        this.contractDet.m_currency = currency;
        this.contractDet.m_secType = securityType;
        this.contractDet.m_exchange = exchange;
    }

    // constructor for contract type FUT
    private void setContractDet(String symbol, String currency, String securityType, String exchange, String expiry) {
        this.contractDet.m_symbol = symbol;
        this.contractDet.m_currency = currency;
        this.contractDet.m_secType = securityType;
        this.contractDet.m_exchange = exchange;
        this.contractDet.m_expiry = expiry;
    }

    // constructor for contract type OPT
    private void setContractDet(String symbol, String currency, String securityType, String exchange, String expiry, String rightType, double strikePrice) {
        this.contractDet.m_symbol = symbol;
        this.contractDet.m_currency = currency;
        this.contractDet.m_secType = securityType;
        this.contractDet.m_exchange = exchange;
        this.contractDet.m_expiry = expiry;
        this.contractDet.m_right = rightType;
        this.contractDet.m_strike = strikePrice;
    }

    // Constructor for contract Type IND
    public void setContractDetInd(String symbol, String currency, String exchange) {
        this.setContractDet(symbol, currency, "IND", exchange);
    }
    
    // Constructor for contract Type STK
    public void setContractDetStk(String symbol, String currency, String exchange) {
        this.setContractDet(symbol, currency, "STK", exchange);
    }
    
    // constructor for contract type FUT
    public void setContractDetFut(String symbol, String currency, String exchange, String expiry) {
        this.setContractDet(symbol, currency, "FUT", exchange, expiry);
    }
    
    // constructor for contract type OPT
    public void setContractDetOpt(String symbol, String currency, String exchange, String expiry, String rightType, double strikePrice) {
        this.setContractDet(symbol, currency, "OPT", exchange, expiry, rightType, strikePrice);
    }    
    
    // constructor for contract type OPT - Put Option
    public void setContractDetCallOption(String symbol, String currency, String exchange, String expiry, double strikePrice) {
        this.setContractDetOpt(symbol, currency, exchange, expiry, "PUT", strikePrice);
    }        

    // constructor for contract type OPT - Call Option
    public void setContractDetPutOption(String symbol, String currency, String exchange, String expiry, double strikePrice) {
        this.setContractDetOpt(symbol, currency, exchange, expiry, "CALL", strikePrice);
    }            
}

// Define class to store snapshot of Bid Ask price
class MyBidAskPriceObjClass {

    int requestId, symbolBidVolume, symbolAskVolume;
    double symbolBidPrice, symbolAskPrice;
    long bidPriceUpdateTime, askPriceUpdateTime;

    public MyBidAskPriceObjClass(int requestId) {
        this.requestId = requestId;
        this.symbolBidVolume = 0;
        this.symbolAskVolume = 0;
        this.symbolBidPrice = 0.0;
        this.symbolAskPrice = 0.0;
        this.bidPriceUpdateTime = -1;
        this.askPriceUpdateTime = -1;
    }

    public int getRequestId() {
        return this.requestId;
    }

    public int getSymbolBidVolume() {
        return this.symbolBidVolume;
    }

    public int getSymbolAskVolume() {
        return this.symbolAskVolume;
    }

    public double getSymbolBidPrice() {
        return this.symbolBidPrice;
    }

    public double getSymbolAskPrice() {
        return this.symbolAskPrice;
    }

    public long getBidPriceUpdateTime() {
        return this.bidPriceUpdateTime;
    }

    public long getAskPriceUpdateTime() {
        return this.askPriceUpdateTime;
    }

    public void setRequestId(int requestId) {
        this.requestId = requestId;
    }

    public void setSymbolBidVolume(int volume) {
        this.symbolBidVolume = volume;
    }

    public void setSymbolAskVolume(int volume) {
        this.symbolAskVolume = volume;
    }

    public void setSymbolBidPrice(double price) {
        this.symbolBidPrice = price;
    }

    public void setSymbolAskPrice(double price) {
        this.symbolAskPrice = price;
    }

    public void setBidPriceUpdateTime(long time) {
        this.bidPriceUpdateTime = time;
    }

    public void setAskPriceUpdateTime(long time) {
        this.askPriceUpdateTime = time;
    }
}

// Define class to get Order Status
class MyOrderStatusObjClass {

    private int orderId;
    private double filledPrice, averagePrice, commissionAmount;
    private int remainingQuantity, filledQuantity;
    private long updateTime;
    private String uniqueExecutionId, orderReference;
    private Contract contractDet = new Contract();
    
    public MyOrderStatusObjClass(int orderId) {
        this.orderId = orderId;
        this.filledPrice = 0.0;
        this.averagePrice = 0.0;
        this.commissionAmount = 0.0;
        this.remainingQuantity = -1;
        this.filledQuantity = 0;
        this.updateTime = -1;
        this.uniqueExecutionId = "";
        this.orderReference = "";
    }

    public int getOrderId() {
        return this.orderId;
    }
    
    public String getUniqueExecutionId() {
        return this.uniqueExecutionId;
    } 
    
    public String getOrderReference() {
        return this.orderReference;
    }      

    public Contract getContractDet() {
        return this.contractDet;
    }
    
    public double getFilledPrice() {
        return this.filledPrice;
    }

    public double getAveragePrice() {
        return this.averagePrice;
    }    

    public double getCommissionAmount() {
        return this.commissionAmount;
    }    
    
    public int getRemainingQuantity() {
        return this.remainingQuantity;
    }

    public int getFilledQuantity() {
        return this.filledQuantity;
    }

    public long getUpdateTime() {
        return this.updateTime;
    }

    public void setOrderId(int orderId) {
        this.orderId = orderId;
    }

    public void setUniqueExecutionId(String executionId) {
        this.uniqueExecutionId = executionId;
    }

    public void setOrderReference(String orderRef) {
        this.orderReference = orderRef;
    }    
       
    public void setFilledPrice(double price) {
        this.filledPrice = price;
    }

    public void setAveragePrice(double price) {
        this.averagePrice = price;
    }
    
    public void setCommissionAmount(double commissionAmt) {
        this.commissionAmount = commissionAmt;
    }
    
    public void setRemainingQuantity(int quantity) {
        this.remainingQuantity = quantity;
    }

    public void setFilledQuantity(int quantity) {
        this.filledQuantity = quantity;
    }

    public void setUpdateTime(long time) {
        this.updateTime = time;
    }

    public void setContractDet(Contract contractDetails) {
        this.contractDet = contractDetails;
    }
    
}

// Define class to store Manual Intervention Signals
class MyManualInterventionClass {

    private int slotNumber, actionIndicator;
    private String targetValue, actionReason;
    public static final int DEFAULT = 0;    
    public static final int SQUAREOFF = 1;
    public static final int UPDATETAKEPROFIT = 2;
    public static final int UPDATESTOPLOSS = 3;
    public static final int STOPMONITORING = 4;

    public MyManualInterventionClass(int slotNumber, String targetValue, int actionIndicator) {
        this.slotNumber = slotNumber;
        this.targetValue = targetValue;
        this.actionIndicator = actionIndicator;
    }

    public int getSlotNumber() {
        return this.slotNumber;
    }

    public String getActionReason() {
        return this.actionReason;
    }
        
    public String getTargetValue() {
        return this.targetValue;
    }

    public int getActionIndicator() {
        return this.actionIndicator;
    }

    public void setSlotNumber(int slotNumber) {
        this.slotNumber = slotNumber;
    }

    public void setActionReason(String actionReason) {
        this.actionReason = actionReason;
    }
    
    public void setTargetValue(String targetValue) {
        this.targetValue = targetValue;
    }

    public void setActionIndicator(int actionIndicator) {
        this.actionIndicator = actionIndicator;
    }
}

public class MyUtils {

    public Object lockOpenPosSlotChange = new Object();
    public Object lockClosedPosSlotChange = new Object();    

    public void waitForNSeconds(int numSeconds) {

        try {
            Thread.sleep(numSeconds * 1000);
        } catch (InterruptedException ex) {
            Thread.currentThread().interrupt();
        }

    } //End of method    

    public void waitForNMiliSeconds(int numMiliSeconds) {

        try {
            Thread.sleep(numMiliSeconds);
        } catch (InterruptedException ex) {
            Thread.currentThread().interrupt();
        }

    } //End of method 

    public void waitForStartTime(int startTimeHHMM, TimeZone timeZone, String customMessage, boolean debugFlag) {

        TimeZone.setDefault(timeZone);
        Calendar timeNow = Calendar.getInstance(timeZone);
        // Provision to wait time of wait is over
        while (Integer.parseInt(String.format("%1$tH%1$tM", timeNow)) < startTimeHHMM) {
            if (debugFlag) {
                System.out.println("Waiting for " + customMessage + " At :" + String.format("%1$tY%1$tm%1$td%1$tH%1$tM%1$tS", timeNow));
            }
            // Wait for 20 seconds before checking again
            waitForNSeconds(20);
            timeNow = Calendar.getInstance(timeZone);
        }
        if (debugFlag) {
            System.out.println("Wait for " + customMessage + " over at:" + String.format("%1$tY%1$tm%1$td%1$tH%1$tM%1$tS", timeNow));
        }
    } // End of method

    public boolean fallsOnExchangeHoliday(String customMessage, String holidayList, Calendar timeToCheck, boolean debugFlag) {

        boolean exchangeHolidayStatus = false;
        String exchangeHolidayList[] = holidayList.split(",");
        // Provision to check if current Date falls on any of the Exchange Holidays
        for (int index = 0; index < exchangeHolidayList.length; index++) {
            if (String.format("%1$tY%1$tm%1$td", timeToCheck).equalsIgnoreCase(exchangeHolidayList[index])
                    || (timeToCheck.get(Calendar.DAY_OF_WEEK) == Calendar.SATURDAY)
                    || (timeToCheck.get(Calendar.DAY_OF_WEEK) == Calendar.SUNDAY)) {
                if (debugFlag) {
                    System.out.println(customMessage + " Date today :" + String.format("%1$tY%1$tm%1$td", timeToCheck));
                }
                exchangeHolidayStatus = true;
            }
        }

        return (exchangeHolidayStatus);
    } // End of method

    public void waitForMarketsToOpen(int marketOpeningTimeHHMM, TimeZone timeZone, boolean debugFlag) {

        TimeZone.setDefault(timeZone);
        Calendar timeNow = Calendar.getInstance(timeZone);
        // Provision to wait till market opens for NSE
        while (Integer.parseInt(String.format("%1$tH%1$tM", timeNow)) < marketOpeningTimeHHMM) {
            if (debugFlag) {
                System.out.println("Markets Not Open Yet at:" + String.format("%1$tY%1$tm%1$td%1$tH%1$tM%1$tS", timeNow));
            }
            // Wait for 20 seconds before checking again
            waitForNSeconds(20);
            timeNow = Calendar.getInstance(timeZone);
        }
        if (debugFlag) {
            System.out.println("Markets Are Open now at:" + String.format("%1$tY%1$tm%1$td%1$tH%1$tM%1$tS", timeNow));
        }
    } // End of method

    public boolean marketIsOpen(int marketClosingTimeHHMM, TimeZone timeZone, boolean debugFlag) {

        boolean returnValue = true;
        TimeZone.setDefault(timeZone);
        Calendar timeNow = Calendar.getInstance(timeZone);
        // return false if time has reached outside market hours for NSE
        if (Integer.parseInt(String.format("%1$tH%1$tM", timeNow)) >= marketClosingTimeHHMM) {
            returnValue = false;
            if (debugFlag) {
                System.out.println("Reached End of Day at :" + String.format("%1$tY%1$tm%1$td%1$tH%1$tM%1$tS", timeNow));
            }
        }

        //returnValue = true; // For Testing Purposes
        return (returnValue);

    } // End of method

    public String popKeyValueFromQueueRedis(JedisPool jedisPool, String queueName, int timeOut, boolean debugFlag) {

        List<String> localList = null;
        boolean caughtException = false;
        Jedis jedis = jedisPool.getResource();

        try {
            localList = jedis.brpop(timeOut, queueName);
        } catch (Exception ex) {
            caughtException = true;
            // do nothing
            System.out.println("Exception Caught. Message : " + ex.getMessage());
            if (debugFlag) {
                System.out.println("Caught Exception while popping from queue " + queueName);
            }
        }
        jedisPool.returnResource(jedis);

        if ((localList != null) && (!caughtException)) {
            if (debugFlag) {
                System.out.println("Received Message for  " + queueName + " KEY: " + localList.get(0) + " VALUE: " + localList.get(1));
            }
            return (localList.get(1));
        } else {
            return (null);
        }
    }

    public String getHashMapValueFromRedis(JedisPool jedisPool, String hashKeyName, String fieldName, boolean debugFlag) {

        String retValue = null;
        int noOfAttempts = 1;
        boolean exceptionCaught = true;
        Jedis jedis = jedisPool.getResource();

        while ((noOfAttempts <= 9) && (exceptionCaught)) {
            noOfAttempts++;
            exceptionCaught = false;
            String localString = null;
            try {
                localString = jedis.hget(hashKeyName, fieldName);
            } catch (Exception ex) {
                // print exception caught
                exceptionCaught = true;
                System.out.println("Exception Caught. Message : " + ex.getMessage());
            }
            if (localString != null && localString.length() > 0) {
                retValue = localString;
            }
            if (exceptionCaught) {
                if (debugFlag) {
                    System.out.println("Caught Exception while reading " + fieldName + " from Redis hashKey " + hashKeyName + " on attempt number " + noOfAttempts);
                }
                // wait for few milli second before retrying or exiting
                waitForNMiliSeconds(100);
            } else {
                if (debugFlag) {
                    //System.out.println("Read Value from Redis fielname " + fieldName + " from Redis hashKey " + hashKeyName + " as " + localString);
                }
            }
        }

        jedisPool.returnResource(jedis);
        return (retValue);
    } //End of method  

    public void defragmentOpenPositionsQueue(JedisPool jedisPool, String redisConfigurationKey, boolean debugFlag) {

        Jedis jedis;
        String openPositionsQueueKeyName = getHashMapValueFromRedis(jedisPool, redisConfigurationKey, "OPENPOSITIONSQUEUE", debugFlag);
        int maxNumPositions = Integer.parseInt(getHashMapValueFromRedis(jedisPool, redisConfigurationKey, "MAXNUMLONGPOSITIONS", debugFlag))
                + Integer.parseInt(getHashMapValueFromRedis(jedisPool, redisConfigurationKey, "MAXNUMSHORTPOSITIONS", debugFlag));
        if (debugFlag) {
            System.out.println(" Maximum Allowed Positions " + maxNumPositions);
        }

        Map<String, String> updatedMap = new HashMap<String, String>();
        int targetSlotNum = 1;
        jedis = jedisPool.getResource();
        try {
            // retrieve open position map from redis  
            Map<String, String> retrieveMap = jedis.hgetAll(openPositionsQueueKeyName);
            for (String keyMap : retrieveMap.keySet()) {
                updatedMap.put(Integer.toString(targetSlotNum), retrieveMap.get(keyMap));
                if (debugFlag) {
                    System.out.println(" Got Open Positions for Slot Number : " + keyMap + " position details " + retrieveMap.get(keyMap));
                    System.out.println(" Targeting move from Slot Number : " + keyMap + " to " + targetSlotNum);
                }
                targetSlotNum++;
            }
        } catch (JedisException e) {
            //if something wrong happen, return it back to the pool
            if (null != jedis) {
                jedisPool.returnBrokenResource(jedis);
                jedis = null;
            }
        } finally {
            //Return the Jedis instance to the pool once finished using it  
            if (null != jedis) {
                jedisPool.returnResource(jedis);
            }
        }

        synchronized(lockOpenPosSlotChange) {
            jedis = jedisPool.getResource();
            try {
                //empty the Redis Map 
                // retrieve open position map from redis  
                Map<String, String> retrieveMap = jedis.hgetAll(openPositionsQueueKeyName);
                for (String keyMap : retrieveMap.keySet()) {
                    jedis.hdel(openPositionsQueueKeyName, keyMap);
                }
                //save updated map to redis                
                jedis.hmset(openPositionsQueueKeyName, updatedMap);
            } catch (JedisException e) {
                //if something wrong happen, return it back to the pool
                if (null != jedis) {
                    jedisPool.returnBrokenResource(jedis);
                    jedis = null;
                }
            } finally {
                //Return the Jedis instance to the pool once finished using it  
                if (null != jedis) {
                    jedisPool.returnResource(jedis);
                }
            }            
        }


    } // End of method

    public void moveCurrentClosedPositions2ArchiveQueue(JedisPool jedisPool, String redisConfigurationKey, boolean debugFlag) {

        String closedPositionsQueueKeyName = getHashMapValueFromRedis(jedisPool, redisConfigurationKey, "CLOSEDPOSITIONSQUEUE", debugFlag);
        String archivePositionsQueueKeyName = getHashMapValueFromRedis(jedisPool, redisConfigurationKey, "PASTCLOSEDPOSITIONSQUEUE", debugFlag);

        // Find the field value of hash to update exited position
        int archivedQueueIndex = 1;
        while (checkIfExistsHashMapField(jedisPool, archivePositionsQueueKeyName, Integer.toString(archivedQueueIndex), debugFlag)) {
            archivedQueueIndex++;
        }

        int indexNumber = 1;
        while (checkIfExistsHashMapField(jedisPool, closedPositionsQueueKeyName, Integer.toString(indexNumber), debugFlag)) {
            // Move from Closed Positions Queue to Archived Positions Queue
            // read the trade details from closed positions queue
            String tradeDetails = getHashMapValueFromRedis(jedisPool, closedPositionsQueueKeyName, Integer.toString(indexNumber), debugFlag);
            // Update the Archived Queue at archive Index
            synchronized(lockClosedPosSlotChange) {
                Jedis jedis = jedisPool.getResource();
                jedis.hset(archivePositionsQueueKeyName, Integer.toString(archivedQueueIndex), tradeDetails);
                // Delete from Closed Positions Queue
                jedis.hdel(closedPositionsQueueKeyName, Integer.toString(indexNumber));
                jedisPool.returnResource(jedis);
                // Increment the Archived and Closed position Indexes
                archivedQueueIndex++;
                indexNumber++;                
            }

        }

    }

    public int getNextOrderID(JedisPool jedisPool, String keyName, boolean debugFlag) {

        int retOrderID = -1;
        int noOfAttempts = 1;
        boolean exceptionCaught = true;
        long incrementedVal = -1;
        Jedis jedis = jedisPool.getResource();

        while ((noOfAttempts <= 5) && (exceptionCaught)) {
            noOfAttempts++;
            exceptionCaught = false;
            try {
                incrementedVal = jedis.incr(keyName);
            } catch (Exception ex) {
                // print exception caught
                exceptionCaught = true;
                System.out.println("Exception Caught. Message : " + ex.getMessage());
            }
        }
        if (exceptionCaught) {
            if (debugFlag) {
                System.out.println("Caught Exception while incrementing OrderID " + keyName + " on attempt number " + noOfAttempts);
            }
            // wait for few milliseconds before retrying or exiting
            waitForNMiliSeconds(100);
        } else {
            retOrderID = (int) incrementedVal;
        }

        jedisPool.returnResource(jedis);
        return (retOrderID);
    }

    public boolean setNextOrderID(JedisPool jedisPool, String keyName, int orderIdValue, boolean debugFlag) {

        boolean successStatus = false;
        int noOfAttempts = 1;
        boolean exceptionCaught = true;
        Jedis jedis = jedisPool.getResource();

        while ((noOfAttempts <= 5) && (exceptionCaught)) {
            noOfAttempts++;
            exceptionCaught = false;
            try {
                jedis.set(keyName, Integer.toString(orderIdValue));
                if (debugFlag) {
                    System.out.println("Setting order ID filed for Key : " + keyName + " orderId : " + orderIdValue);
                }
            } catch (Exception ex) {
                // print exception caught
                exceptionCaught = true;
                System.out.println("Exception Caught. Message : " + ex.getMessage());
            }
        }
        if (exceptionCaught) {
            if (debugFlag) {
                System.out.println("Caught Exception while Setting OrderID " + keyName + " on attempt number " + noOfAttempts);
            }
            // wait for few milliseconds before retrying or exiting
            waitForNMiliSeconds(100);
        } else {
            successStatus = true;
        }

        jedisPool.returnResource(jedis);
        return (successStatus);
    }

    public String getKeyValueFromRedis(JedisPool jedisPool, String KeyName, boolean debugFlag) {

        String retValue = null;
        int noOfAttempts = 1;
        boolean exceptionCaught = true;
        Jedis jedis = jedisPool.getResource();

        while ((noOfAttempts <= 9) && (exceptionCaught)) {
            noOfAttempts++;
            exceptionCaught = false;
            String localString = null;
            try {
                localString = jedis.get(KeyName);
            } catch (Exception ex) {
                // do nothing
                exceptionCaught = true;
                System.out.println("Exception Caught. Message : " + ex.getMessage());
            }
            if (localString != null && localString.length() > 0) {
                retValue = localString;
            }
            if (exceptionCaught) {
                if (debugFlag) {
                    System.out.println("Caught Exception while reading from Redis Key " + KeyName + " on attempt number " + noOfAttempts);
                }
                // wait for one second before retrying or exiting
                waitForNMiliSeconds(100);
            } else {
                if (debugFlag) {
                    //System.out.println("Read Value from Redis Key " + KeyName + " as " + localString);
                }
            }
        }

        jedisPool.returnResource(jedis);
        return (retValue);
    } //End of method            

    public boolean checkIfExistsHashMapField(JedisPool jedisPool, String hashKeyName, String fieldName, boolean debugFlag) {

        boolean retValue = false;
        int noOfAttempts = 0;
        boolean exceptionCaught = true;
        Jedis jedis = jedisPool.getResource();

        while ((noOfAttempts <= 9) && (exceptionCaught)) {
            noOfAttempts++;
            exceptionCaught = false;
            try {
                retValue = jedis.hexists(hashKeyName, fieldName);
            } catch (Exception ex) {
                // do nothing
                exceptionCaught = true;
                System.out.println("Exception Caught. Message : " + ex.getMessage());
            }
            if (exceptionCaught) {
                if (debugFlag) {
                    System.out.println("Caught Exception checking ifExists " + fieldName + " from Redis hashKey " + hashKeyName + " on attempt number " + noOfAttempts);
                }
                // wait for few milli second before retrying or exiting
                waitForNMiliSeconds(100);
            } else {
                if (debugFlag) {
                    //System.out.println("Checked successfullay ifExists Redis fielname " + fieldName + " from Redis hashKey " + hashKeyName + " as " + retValue);
                }
            }
        }

        jedisPool.returnResource(jedis);
        return (retValue);
    } //End of method  

    int incrementHHMMByNMinute(String timeStampHHMM, int incrementMinutes) {

        int returnValue = 0;

        int hourHH = Integer.parseInt(timeStampHHMM.substring(0, 2));
        int minuteMM = Integer.parseInt(timeStampHHMM.substring(2, 4));

        minuteMM += incrementMinutes;
        if (minuteMM >= 60) {
            hourHH += 1;
            minuteMM -= 60;
        }

        returnValue = hourHH * 100 + minuteMM;

        return (returnValue);
    }

    int calcElapsedBars(JedisPool jedisPool, String startTime, String currentTime, MyExchangeClass exchangeObj, String exchangeHolidayListKeyName, boolean debugFlag) {

        int elapsedBars = 0;

        TimeZone.setDefault(exchangeObj.getExchangeTimeZone());
        SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMddHHmmss");
        Calendar startTS = Calendar.getInstance(exchangeObj.getExchangeTimeZone());
        try {
            startTS.setTime(sdf.parse(startTime));
        } catch (ParseException ex) {
            elapsedBars = -1;
            Logger.getLogger(MyUtils.class.getName()).log(Level.SEVERE, null, ex);
        }
        Calendar currentTS = Calendar.getInstance(exchangeObj.getExchangeTimeZone());
        try {
            currentTS.setTime(sdf.parse(currentTime));
        } catch (ParseException ex) {
            elapsedBars = -1;
            Logger.getLogger(MyUtils.class.getName()).log(Level.SEVERE, null, ex);
        }

        if (elapsedBars == 0) {
            if (startTS.after(currentTS)) {
                elapsedBars = -1;
            } else if (startTS.equals(currentTS)) {
                elapsedBars = 0;
            } else {
                int elapsedMinutes = 0;
                while (startTS.before(currentTS)) {
                    elapsedMinutes++;
                    startTS = incrementTimeStampByOneTradingMinute(startTS, getKeyValueFromRedis(jedisPool, exchangeHolidayListKeyName, false), exchangeObj);
                }
                elapsedBars = elapsedMinutes / 10;
                if (debugFlag) {
                    System.out.println("Elapsed Minutes :" + elapsedMinutes + " Elapsed Bars :" + elapsedBars);
                }
            }

        }

        return (elapsedBars);
    }

    Calendar incrementTimeStampByOneTradingMinute(Calendar timeStamp, String holidayList, MyExchangeClass exchangeObj) {

        Calendar retTimeStamp = timeStamp;

        // Increment time stamp by one minute
        retTimeStamp.add(Calendar.MINUTE, 1);

        int exchangeOpeningHour = exchangeObj.getExchangeStartTimeHHMM() / 100;
        int exchangeOpeningMinute = exchangeObj.getExchangeStartTimeHHMM() % 100;
        int exchangeClosingTimeHHMM = exchangeObj.getExchangeCloseTimeHHMM();

        // Bring the minute to trading range
        if (Integer.parseInt(String.format("%1$tH%1$tM", retTimeStamp)) >= exchangeClosingTimeHHMM) {
            retTimeStamp.add(Calendar.DATE, 1);
            retTimeStamp.set(Calendar.HOUR_OF_DAY, exchangeOpeningHour);
            retTimeStamp.set(Calendar.MINUTE, exchangeOpeningMinute);
        }

        while (fallsOnExchangeHoliday("", holidayList, retTimeStamp, false)) {
            retTimeStamp.add(Calendar.DATE, 1);
        }

        return (retTimeStamp);
    }

    void unblockOpenPositionSlot(JedisPool jedisPool, String queueKeyName, int slotNumber) {

        synchronized (this.lockOpenPosSlotChange) {
            // Empty open positions slot
            Jedis jedis = jedisPool.getResource();
            jedis.hdel(queueKeyName, Integer.toString(slotNumber));
            jedisPool.returnResource(jedis);        
        }

    }

    boolean checkIfStaleMessage(String entryTimeStamp, String currentTimeStamp, int differenceInMinutes) {
        boolean returnValue = false;

        // Compare YYYYMMDD
        if (!entryTimeStamp.substring(0, 8).matches(currentTimeStamp.substring(0, 8))) {
            //if YYYYMMDD are not same then order is stale
            returnValue = true;
        } else {
            // YYYYMMDD is same. Increment HHMM by 5 minutes and check if it more than current.
            try {
                int incrementedTimeHHMM = incrementHHMMByNMinute(entryTimeStamp.substring(8, 12), differenceInMinutes);
                int currentTimeHHMM = Integer.parseInt(currentTimeStamp.substring(8, 12));

                if (incrementedTimeHHMM < currentTimeHHMM) {
                    // it is more than given minutes stale.
                    returnValue = true;
                }
            } catch (Exception ex) {
                System.out.println("Exception caused while converting timestamps to HHMM from string. Defaulting to assume it is not a stale Order " + ex.getMessage());
                returnValue = true;
            }
        }

        return (returnValue);
    }

    void updateAndMoveClosedPositions(JedisPool jedisPool, String openPositionsQueueKeyName, int openPosSlotNumber, String closedPositionsQueueKeyName, double exitSpread, String exitTimeStamp, int exitOrderId, String orderState, String exitReason, boolean debugFlag) {

        Jedis jedis;
        String myLegDetails = getHashMapValueFromRedis(jedisPool, openPositionsQueueKeyName, Integer.toString(openPosSlotNumber), debugFlag);
        // update Redis queues with squared off order details
        TradingObject myTradeObject = new TradingObject(myLegDetails);
        if (exitOrderId > 0) {
            myTradeObject.setTradingContractExitSpread(exitSpread);
        }
        if (exitTimeStamp.length() > 1) {
            myTradeObject.setTradingContractExitTimeStamp(exitTimeStamp);
        }
        if (exitOrderId > 0) {
            myTradeObject.setTradingContractExitOrderIDs(exitOrderId);            
        }
        if (exitReason.length() > 1) {
            myTradeObject.setTradingContractExitReason(exitReason);            
        }        
        if (orderState.length() > 1) {
            myTradeObject.setOrderState(orderState);            
        }

        synchronized (lockClosedPosSlotChange) {
            // Find the field value of hash to update exited position
            int indexNumber = 0;
            boolean found = false;
            while (found == false) {
                indexNumber++;
                if (!checkIfExistsHashMapField(jedisPool, closedPositionsQueueKeyName, Integer.toString(indexNumber), debugFlag)) {
                    found = true;
                }
            }
            // Update the closed position with exit Details Signal
            jedis = jedisPool.getResource();
            jedis.hset(closedPositionsQueueKeyName, Integer.toString(indexNumber), myTradeObject.getCompleteTradingObjectString());
            jedisPool.returnResource(jedis);            
        }
        synchronized (lockOpenPosSlotChange) {
            jedis = jedisPool.getResource();
            // To update Open position queue, delete the key name from Redis Open Position queue to indicate availability of slot.
            jedis.hdel(openPositionsQueueKeyName, Integer.toString(openPosSlotNumber));
            jedisPool.returnResource(jedis);            
        }
    }
    
    void updateExitPositionStatusInQueues(JedisPool jedisPool, String openPositionsQueueKeyName, int openPosSlotNumber, String closedPositionsQueueKeyName, double exitSpread, String exitTimeStamp, int exitOrderId, String orderState, String bidAskDetails, String exitReason, boolean debugFlag) {

        String myLegDetails = getHashMapValueFromRedis(jedisPool, openPositionsQueueKeyName, Integer.toString(openPosSlotNumber), debugFlag);
        if (orderState.equalsIgnoreCase("exitorderfilled")) {
            // update Redis queues with squared off order
            updateClosedPositionsQueueWithCompletelyFilledOrderStatus(jedisPool, closedPositionsQueueKeyName, myLegDetails, exitSpread, exitTimeStamp, exitOrderId, bidAskDetails, exitReason, debugFlag);
            synchronized (lockOpenPosSlotChange) {
                // To update Open position queue, delete the key name from Redis Open Position queue to indicate availability of slot.
                Jedis jedis = jedisPool.getResource();
                jedis.hdel(openPositionsQueueKeyName, Integer.toString(openPosSlotNumber));
                jedisPool.returnResource(jedis);                
            }
        } else if (orderState.equalsIgnoreCase("exitordersenttoexchange")) {
            // update Redis queues with squared off order
            updateOpenPositionsQueueWithIncompletelyFilledOrderStatus(jedisPool, openPositionsQueueKeyName, openPosSlotNumber, myLegDetails, exitSpread, exitTimeStamp, exitOrderId, bidAskDetails, exitReason, debugFlag);
        }
    }

    void updateOpenPositionsQueueWithIncompletelyFilledOrderStatus(JedisPool jedisPool, String queueKeyName, int slotNumber, String updateDetails, double exitSpread, String exitTimeStamp, int exitOrderId, String bidAskDetails, String exitReason, boolean debugFlag) {

        TradingObject myTradeObject = new TradingObject(updateDetails);

        myTradeObject.setTradingContractExitReason(exitReason);        
        myTradeObject.setTradingContractExitSpread(exitSpread);
        myTradeObject.setTradingContractExitTimeStamp(exitTimeStamp);
        myTradeObject.setTradingContractExitOrderIDs(exitOrderId);
        myTradeObject.setTradingContractExitBidAskFillDetails(bidAskDetails);

        if (debugFlag) {
            System.out.println(String.format("%1$tY%1$tm%1$td:%1$tH:%1$tM:%1$tS ", Calendar.getInstance()) + "Updating Open Position Details for Incompletely Filled Order for Slot Number : " + slotNumber);
        }

        synchronized (lockOpenPosSlotChange) {
            // Update the closed position with exit Details Signal
            Jedis jedis = jedisPool.getResource();
            jedis.hset(queueKeyName, Integer.toString(slotNumber), myTradeObject.getCompleteTradingObjectString());
            jedisPool.returnResource(jedis);            
        }

    }

    void updateClosedPositionsQueueWithCompletelyFilledOrderStatus(JedisPool jedisPool, String queueKeyName, String updateDetails, double exitSpread, String exitTimeStamp, int exitOrderId, String bidAskDetails, String exitReason, boolean debugFlag) {

        TradingObject myTradeObject = new TradingObject(updateDetails);
        myTradeObject.setTradingContractExitReason(exitReason);
        myTradeObject.setTradingContractExitSpread(exitSpread);
        myTradeObject.setTradingContractExitTimeStamp(exitTimeStamp);
        myTradeObject.setTradingContractExitOrderIDs(exitOrderId);
        myTradeObject.setTradingContractExitBidAskFillDetails(bidAskDetails);

        if (debugFlag) {
            System.out.println(String.format("%1$tY%1$tm%1$td:%1$tH:%1$tM:%1$tS ", Calendar.getInstance()) + "Updating and Closing Position Details for Completely Filled Order for current Slot Number.");
        }

        synchronized (lockClosedPosSlotChange) {
            // Find the field value of hash to update exited position
            int indexNumber = 0;
            boolean found = false;
            while (found == false) {
                indexNumber++;
                if (!checkIfExistsHashMapField(jedisPool, queueKeyName, Integer.toString(indexNumber), debugFlag)) {
                    found = true;
                }
            }
            // Update the closed position with exit Details Signal
            Jedis jedis = jedisPool.getResource();
            jedis.hset(queueKeyName, Integer.toString(indexNumber), myTradeObject.getCompleteTradingObjectString());
            jedisPool.returnResource(jedis);            
        }
    }
    
    public void getOrderDetails2LocalDB(JedisPool jedisPool, String redisConfigurationKey, IBInteraction ibInteractionClient, boolean debugFlag) {

        int requestId = ibInteractionClient.requestExecutionDetailsHistorical(30);
        // wait till details are received OR for timeuut to happen
        int timeOut = 0;
        while ((timeOut < 301)
                && (!(ibInteractionClient.requestsCompletionStatus.get(requestId)) ) ) {
            waitForNSeconds(5);
            timeOut = timeOut + 5;
        }

        Jedis jedis = jedisPool.getResource();
        try {
            // retrieve map from redis  
            Map<String, String> orderId2UniqueExecutionMapping = jedis.hgetAll("ORDERID2UNIQUEEXECUTIONIDMAPPING");
            Map<String, String> orderExecutionRecords = jedis.hgetAll("IBORDEREXECUTIONRECORDS"); 

            for (int orderId : ibInteractionClient.myOrderStatusDetails.keySet()) {            
                orderId2UniqueExecutionMapping.put(Integer.toString(orderId), ibInteractionClient.myOrderStatusDetails.get(orderId).getUniqueExecutionId());

                // Form the string to record Execution details in Local DB - Redis in this case
                String orderExecutionDetails = 
                        Integer.toString(orderId) + "," +
                        Double.toString(ibInteractionClient.myOrderStatusDetails.get(orderId).getFilledPrice()) + "," +
                        Integer.toString(ibInteractionClient.myOrderStatusDetails.get(orderId).getFilledQuantity()) + "," +
                        Double.toString(ibInteractionClient.myOrderStatusDetails.get(orderId).getAveragePrice()) + "," +
                        Integer.toString(ibInteractionClient.myOrderStatusDetails.get(orderId).getRemainingQuantity()) + "," +
                        Double.toString(ibInteractionClient.myOrderStatusDetails.get(orderId).getCommissionAmount()) + "," +
                        convertTime(ibInteractionClient.myOrderStatusDetails.get(orderId).getUpdateTime()) + "," +
                        ibInteractionClient.myOrderStatusDetails.get(orderId).getOrderReference() + "," +
                        ibInteractionClient.myOrderStatusDetails.get(orderId).getContractDet().m_symbol + "," +                        
                        ibInteractionClient.myOrderStatusDetails.get(orderId).getContractDet().m_secType + "," +
                        ibInteractionClient.myOrderStatusDetails.get(orderId).getContractDet().m_expiry + "," +
                        ibInteractionClient.myOrderStatusDetails.get(orderId).getContractDet().m_right + "," +
                        Double.toString(ibInteractionClient.myOrderStatusDetails.get(orderId).getContractDet().m_strike) + "," +
                        ibInteractionClient.myOrderStatusDetails.get(orderId).getContractDet().m_secId + "," +
                        ibInteractionClient.myOrderStatusDetails.get(orderId).getContractDet().m_secIdType + "," +                        
                        ibInteractionClient.myOrderStatusDetails.get(orderId).getContractDet().m_exchange + "," +
                        ibInteractionClient.myOrderStatusDetails.get(orderId).getContractDet().m_currency + "," +
                        ibInteractionClient.myOrderStatusDetails.get(orderId).getContractDet().m_primaryExch + "," +
                        ibInteractionClient.myOrderStatusDetails.get(orderId).getContractDet().m_localSymbol;                        
                orderExecutionRecords.put(ibInteractionClient.myOrderStatusDetails.get(orderId).getUniqueExecutionId(), orderExecutionDetails);
            }
            jedis.hmset("ORDERID2UNIQUEEXECUTIONIDMAPPING", orderId2UniqueExecutionMapping);
            jedis.hmset("IBORDEREXECUTIONRECORDS", orderExecutionRecords);
        } catch (JedisException e) {
            //if something wrong happen, return it back to the pool
            if (null != jedis) {
                jedisPool.returnBrokenResource(jedis);
                jedis = null;
            }
        } finally {
            //Return the Jedis instance to the pool once finished using it  
            if (null != jedis) {
                jedisPool.returnResource(jedis);
            }
        }               
    }

    public String convertTime(long time){
        Date date = new Date(time);
        Format format = new SimpleDateFormat("yyyyMMddHHmmss");
        return format.format(date);
    }                    
    
}
