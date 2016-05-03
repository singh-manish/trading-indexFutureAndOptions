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

import java.util.Calendar;
import java.util.Map;
import java.util.TimeZone;
import redis.clients.jedis.*;
import redis.clients.jedis.exceptions.JedisException;

/**
 * @author Manish Kumar Singh
 */
public class MonitorEntrySignals extends Thread {

    private Thread t;
    private String threadName = "MonitoringEntrySignalsThread";
    private boolean debugFlag;
    private JedisPool jedisPool;

    private String redisConfigurationKey;

    private IBInteraction ibInteractionClient;
    private MyExchangeClass myExchangeObj;

    private MyUtils myUtils;

    private String strategyName = "singlestr01";
    private String openPositionsQueueKeyName = "INRSTR01OPENPOSITIONS";
    private String closedPositionsQueueKeyName = "INRSTR01CLOSEDPOSITIONS";
    private String entrySignalsQueueKeyName = "INRSTR01ENTRYSIGNALS";
    private String confOrderType = "MARKET";
    private String duplicateComboAllowed = "yes";
    private String duplicateLegAllowed = "yes";
    //private String signalSymbolType = "FUT"; // STK or FUT or OPT

    private int MAXLONGPOSITIONS = 2;
    private int MAXSHORTPOSITIONS = 2;

    private int nextOpenSlotNumber = 6;

    private int MAXNUMENTRIESINADAY = 15;
    private double NOFURTHERPOSITIONTAKEPROFITLIMIT = 10000.0;
    private double NOFURTHERPOSITIONSTOPLOSSLIMIT = -10000.0;

    public String exchangeHolidayListKeyName;
    private int minimumMoratoriumForPosition = 37;
    
    MonitorEntrySignals(String name, JedisPool redisConnectionPool, String redisConfigKey, MyUtils utils, MyExchangeClass exchangeObj, IBInteraction ibInterClient, boolean debugIndicator) {

        threadName = name;
        debugFlag = debugIndicator;
        jedisPool = redisConnectionPool;
        ibInteractionClient = ibInterClient;
        myUtils = utils;
        redisConfigurationKey = redisConfigKey;

        myExchangeObj = exchangeObj;
        TimeZone.setDefault(myExchangeObj.getExchangeTimeZone());

        //"singlestr01", "INRSTR01OPENPOSITIONS", "INRSTR01ENTRYSIGNALS"        
        strategyName = myUtils.getHashMapValueFromRedis(jedisPool, redisConfigurationKey, "STRATEGYNAME", false);
        openPositionsQueueKeyName = myUtils.getHashMapValueFromRedis(jedisPool, redisConfigurationKey, "OPENPOSITIONSQUEUE", false);
        closedPositionsQueueKeyName = myUtils.getHashMapValueFromRedis(jedisPool, redisConfigurationKey, "CLOSEDPOSITIONSQUEUE", false);
        entrySignalsQueueKeyName = myUtils.getHashMapValueFromRedis(jedisPool, redisConfigurationKey, "ENTRYSIGNALSQUEUE", false);
        confOrderType = myUtils.getHashMapValueFromRedis(jedisPool, redisConfigurationKey, "ENTRYORDERTYPE", false);

        duplicateComboAllowed = myUtils.getHashMapValueFromRedis(jedisPool, redisConfigurationKey, "ALLOWDUPLICATECOMBOPOSITIONS", false);
        duplicateLegAllowed = myUtils.getHashMapValueFromRedis(jedisPool, redisConfigurationKey, "ALLOWDUPLICATELEGPOSITIONS", false);

        minimumMoratoriumForPosition = Integer.parseInt(myUtils.getHashMapValueFromRedis(jedisPool, redisConfigurationKey, "MINIMUMMORATORIUMFORPOSITION", false));

        exchangeHolidayListKeyName = myUtils.getHashMapValueFromRedis(jedisPool, redisConfigurationKey, "EXCHANGEHOLIDAYLISTKEYNAME", false);

        MAXLONGPOSITIONS = Integer.parseInt(myUtils.getHashMapValueFromRedis(jedisPool, redisConfigurationKey, "MAXNUMLONGPOSITIONS", false));
        MAXSHORTPOSITIONS = Integer.parseInt(myUtils.getHashMapValueFromRedis(jedisPool, redisConfigurationKey, "MAXNUMSHORTPOSITIONS", false));

        nextOpenSlotNumber = getMinimumOpenPositionSlotNumber(openPositionsQueueKeyName, 1);
        // Debug Message
        System.out.println(String.format("%1$tY%1$tm%1$td:%1$tH:%1$tM:%1$tS ", Calendar.getInstance(myExchangeObj.getExchangeTimeZone())) + "Info : Monitoring Entry Signals for Strategy " + strategyName + " confOrderType " + confOrderType + " nextSlotNum " + nextOpenSlotNumber + " MAXLONGPOSITIONS " + MAXLONGPOSITIONS + " MAXSHORTPOSITIONS " + MAXSHORTPOSITIONS + " queue name " + entrySignalsQueueKeyName);

    }

    private int getMinimumOpenPositionSlotNumber(String queueKeyName, int minimumSlotNum) {

        // Find the fields value of hash to update entered position
        Jedis jedis;
        int slotNumber = minimumSlotNum;

        jedis = jedisPool.getResource();
        try {
            // retrieve open position map from redis  
            Map<String, String> retrieveMap = jedis.hgetAll(queueKeyName);
            for (String keyMap : retrieveMap.keySet()) {
                slotNumber = Math.max(slotNumber, Integer.parseInt(keyMap));
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
        return (slotNumber);
    }

    void blockOpenPositionSlot(String queueKeyName, String comboDetails, int slotNumber) {

        TradingObject myTradingObject = new TradingObject(comboDetails);

        myTradingObject.initiateAndValidate();
        myTradingObject.setOrderState("openpositionslotblocked");

        Jedis jedis = jedisPool.getResource();
        jedis.hset(openPositionsQueueKeyName, Integer.toString(slotNumber), myTradingObject.getCompleteTradingObjectString());
        jedisPool.returnResource(jedis);
    }

    boolean withinEntryOrderTimeRange(String entryTimeStamp) {
        boolean returnValue;

        Calendar timeNow = Calendar.getInstance(myExchangeObj.getExchangeTimeZone());
        int firstEntryOrderTime = 940;
        int lastEntryOrderTime = 1521;
        String firstEntryOrderTimeConfigValue = myUtils.getHashMapValueFromRedis(jedisPool, redisConfigurationKey, "FIRSTENTRYORDERTIME", debugFlag);
        String lastEntryOrderTimeConfigValue = myUtils.getHashMapValueFromRedis(jedisPool, redisConfigurationKey, "LASTENTRYORDERTIME", debugFlag);
        if ((firstEntryOrderTimeConfigValue != null) && (firstEntryOrderTimeConfigValue.length() > 0)) {
            firstEntryOrderTime = Integer.parseInt(firstEntryOrderTimeConfigValue);
        }
        if ((lastEntryOrderTimeConfigValue != null) && (lastEntryOrderTimeConfigValue.length() > 0)) {
            lastEntryOrderTime = Integer.parseInt(lastEntryOrderTimeConfigValue);
        }
        // Provision for checking if time is within limits for entry order time
        if ((Integer.parseInt(String.format("%1$tH%1$tM", timeNow)) >= firstEntryOrderTime)
                && (Integer.parseInt(String.format("%1$tH%1$tM", timeNow)) <= lastEntryOrderTime)) {
            returnValue = true;
            // Debug Message
            System.out.println(String.format("%1$tY%1$tm%1$td:%1$tH:%1$tM:%1$tS ", Calendar.getInstance(myExchangeObj.getExchangeTimeZone())) + "Within entry Order Time Limits Range. Time Now :" + String.format("%1$tY%1$tm%1$td%1$tH%1$tM%1$tS", timeNow) + " first " + firstEntryOrderTimeConfigValue + " last " + lastEntryOrderTimeConfigValue);
        } else {
            returnValue = false;
            // Debug Message
            System.out.println(String.format("%1$tY%1$tm%1$td:%1$tH:%1$tM:%1$tS ", Calendar.getInstance(myExchangeObj.getExchangeTimeZone())) + "Outside entry Order Time Limits Range. Time Now :" + String.format("%1$tY%1$tm%1$td%1$tH%1$tM%1$tS", timeNow) + " first " + firstEntryOrderTimeConfigValue + " last " + lastEntryOrderTimeConfigValue);
        }

        if (myUtils.checkIfStaleMessage(entryTimeStamp, String.format("%1$tY%1$tm%1$td%1$tH%1$tM%1$tS", timeNow), 5)) {
            returnValue = false;
            // Debug Message
            System.out.println(String.format("%1$tY%1$tm%1$td:%1$tH:%1$tM:%1$tS ", Calendar.getInstance(myExchangeObj.getExchangeTimeZone())) + "Stale Order by more than 5 minutes. Entry Time Stamp " + entryTimeStamp + " Time Now :" + String.format("%1$tY%1$tm%1$td%1$tH%1$tM%1$tS", timeNow));
        }

        return (returnValue);
    }

    boolean withinStipulatedCurrentPnLForToday(Integer maxNumOpenPos, String openPosQueueKeyName, String closedPosQueueKeyName, double dayTakeProfitLimit, double dayStopLossLimit) {
        boolean returnValue = true;

        double roundTripBrokerage = 160.0;
        double currentDayPnL = 0.0;
        // Go through first maxNumOpenPos slots to check for open positions
        int slotNumber = 1;
        while (slotNumber <= maxNumOpenPos) {
            if (myUtils.checkIfExistsHashMapField(jedisPool, openPosQueueKeyName, Integer.toString(slotNumber), debugFlag)) {
                // Since position exists, get details
                TradingObject myTradeObject = new TradingObject(myUtils.getHashMapValueFromRedis(jedisPool, openPosQueueKeyName, Integer.toString(slotNumber), debugFlag));
                if ((myTradeObject.getTradingContractLastUpdatedTimeStamp().length() > 12)
                        && !(myTradeObject.getOrderState().equalsIgnoreCase("openpositionslotblocked"))
                        && !(myTradeObject.getOrderState().equalsIgnoreCase("entryorderinitiated"))) {
                    
                    if (myTradeObject.getTradingContractType().equalsIgnoreCase("OPT")) {
                        roundTripBrokerage = 15 * 2 * Math.abs(myTradeObject.getSideAndSize()) ; // For Options assume it ot be INR 15 per lotsize per trade                        
                    } else {
                        roundTripBrokerage = Double.parseDouble(myTradeObject.getTradingContractEntrySpread()) * (0.01 + 0.0019 + 0.0001) / 100;
                        if (myTradeObject.getEntryTimeStamp().substring(0, 8).matches(myTradeObject.getTradingContractLastUpdatedTimeStamp().substring(0, 8))) {
                            roundTripBrokerage = roundTripBrokerage + Double.parseDouble(myTradeObject.getTradingContractLastKnownSpread()) * (0.01 + 0.0019 + 0.0001) / 100;
                        } else {
                            roundTripBrokerage = roundTripBrokerage + Double.parseDouble(myTradeObject.getTradingContractLastKnownSpread()) * (0.01 + 0.0019 + 0.0001 + 0.01) / 100;
                        }                        
                    }
                    if (myTradeObject.getSideAndSize() > 0) {
                        currentDayPnL += Double.parseDouble(myTradeObject.getTradingContractLastKnownSpread()) - Double.parseDouble(myTradeObject.getTradingContractEntrySpread()) - 0.5 * roundTripBrokerage;
                    } else if (myTradeObject.getSideAndSize() < 0) {
                        currentDayPnL += Double.parseDouble(myTradeObject.getTradingContractEntrySpread()) - Double.parseDouble(myTradeObject.getTradingContractLastKnownSpread()) - 0.5 * roundTripBrokerage;
                    }
                }
            }
            slotNumber++;
        }

        // Go through closed position slots to check for todays closed positions
        slotNumber = 1;
        while (myUtils.checkIfExistsHashMapField(jedisPool, closedPosQueueKeyName, Integer.toString(slotNumber), debugFlag)) {
            // Since position exists in closedQueue, get details
            TradingObject myTradeObject = new TradingObject(myUtils.getHashMapValueFromRedis(jedisPool, closedPosQueueKeyName, Integer.toString(slotNumber), debugFlag));
            roundTripBrokerage = Double.parseDouble(myTradeObject.getTradingContractEntrySpread()) * (0.01 + 0.0019 + 0.0001) / 100;            
            if (myTradeObject.getTradingContractType().equalsIgnoreCase("OPT")) {
                roundTripBrokerage = 15 * 2 * Math.abs(myTradeObject.getSideAndSize()) ; // For Options assume it ot be INR 15 per lotsize per trade                        
            } else {
                roundTripBrokerage = Double.parseDouble(myTradeObject.getTradingContractEntrySpread()) * (0.01 + 0.0019 + 0.0001) / 100;
                if (myTradeObject.getEntryTimeStamp().substring(0, 8).matches(myTradeObject.getTradingContractExitTimeStamp().substring(0, 8))) {
                    roundTripBrokerage = roundTripBrokerage + Double.parseDouble(myTradeObject.getTradingContractExitSpread()) * (0.01 + 0.0019 + 0.0001) / 100;
                } else {
                    roundTripBrokerage = roundTripBrokerage + Double.parseDouble(myTradeObject.getTradingContractExitSpread()) * (0.01 + 0.0019 + 0.0001) / 100;
                }                        
            }            
            if (myTradeObject.getSideAndSize() > 0) {
                currentDayPnL += Double.parseDouble(myTradeObject.getTradingContractExitSpread()) - Double.parseDouble(myTradeObject.getTradingContractEntrySpread()) - roundTripBrokerage;
            } else if (myTradeObject.getSideAndSize() < 0) {
                currentDayPnL += Double.parseDouble(myTradeObject.getTradingContractEntrySpread()) - Double.parseDouble(myTradeObject.getTradingContractExitSpread()) - roundTripBrokerage;
            }
            slotNumber++;
        }

        if ((currentDayPnL > dayTakeProfitLimit) || (currentDayPnL < dayStopLossLimit)) {
            returnValue = false;
        }

        // Debug Message
        System.out.println(String.format("%1$tY%1$tm%1$td:%1$tH:%1$tM:%1$tS ", Calendar.getInstance(myExchangeObj.getExchangeTimeZone())) + "Info : DaysPnL :" + currentDayPnL + " dayTakeProfitLimit : " + dayTakeProfitLimit + " dayStopLossLimit : " + dayStopLossLimit + " returning : " + returnValue);

        return (returnValue);
    }

    boolean checkExistingOpenPositions(String openPositionsQueueKeyName, String signalReceived) {

        boolean returnValue = true;
        boolean alreadyExisting = false;
        Jedis jedis;
        int numOpenLongPositions = 0;
        int numOpenShortPositions = 0;

        TradingObject tradingSignal = new TradingObject(signalReceived);
                
        // Go through all open position slots to check for existance of Current Signal        
        jedis = jedisPool.getResource();
        try {
            // retrieve open position map from redis  
            Map<String, String> retrieveMap = jedis.hgetAll(openPositionsQueueKeyName);
            for (String keyMap : retrieveMap.keySet()) {
                // Do Stuff here
                TradingObject myTradeObject = new TradingObject(retrieveMap.get(keyMap));
                if (myTradeObject.getSideAndSize() < 0 ) {
                    numOpenShortPositions++;  // update number of Open Short positions
                }
                if (myTradeObject.getSideAndSize() > 0 ) {
                    numOpenLongPositions++; // update number of Open Long positions
                }                
                if ((myTradeObject.getTradingObjectName().equalsIgnoreCase(tradingSignal.getTradingObjectName())) && 
                        (myTradeObject.getTradingContractType().equalsIgnoreCase(tradingSignal.getTradingContractType())) &&
                        (!duplicateComboAllowed.equalsIgnoreCase("yes")) ) {
                    if (myTradeObject.getTradingContractType().equalsIgnoreCase("STK")) {
                        if (myTradeObject.getTradingContractUnderlyingName().equalsIgnoreCase(tradingSignal.getTradingContractUnderlyingName())) {
                            // new position already exists
                            alreadyExisting = true;                            
                        }                        
                    } else if (myTradeObject.getTradingContractType().equalsIgnoreCase("FUT")) {
                        if (myTradeObject.getTradingContractUnderlyingName().equalsIgnoreCase(tradingSignal.getTradingContractUnderlyingName())) {
                            // new position already exists
                            alreadyExisting = true;                            
                        }                        
                    } else if (myTradeObject.getTradingContractType().equalsIgnoreCase("OPT")) {
                        if ((myTradeObject.getTradingContractUnderlyingName().equalsIgnoreCase(tradingSignal.getTradingContractUnderlyingName() )) &&
                             (myTradeObject.getTradingContractOptionRightType().equalsIgnoreCase(tradingSignal.getTradingContractOptionRightType())) &&
                             (myTradeObject.getTradingContractOptionStrike() == tradingSignal.getTradingContractOptionStrike()) ) {
                            // new position already exists
                            alreadyExisting = true;                        
                        }
                    }
                }
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

        if ((alreadyExisting) ||
                ((tradingSignal.getSideAndSize() > 0) && (numOpenLongPositions >= MAXLONGPOSITIONS)) || 
                ((tradingSignal.getSideAndSize() < 0) && (numOpenShortPositions >= MAXSHORTPOSITIONS))                
            ) {
            returnValue = false;
        }

        // Debug Message
        System.out.println(String.format("%1$tY%1$tm%1$td:%1$tH:%1$tM:%1$tS ", Calendar.getInstance(myExchangeObj.getExchangeTimeZone())) + "numOpenLongPositions : " + numOpenLongPositions + " against Allowed LONG positions : " + MAXLONGPOSITIONS);
        System.out.println(String.format("%1$tY%1$tm%1$td:%1$tH:%1$tM:%1$tS ", Calendar.getInstance(myExchangeObj.getExchangeTimeZone())) + "numOpenShortPositions : " + numOpenShortPositions + " against Allowed SHORT positions : " + MAXSHORTPOSITIONS);
        System.out.println(String.format("%1$tY%1$tm%1$td:%1$tH:%1$tM:%1$tS ", Calendar.getInstance(myExchangeObj.getExchangeTimeZone())) + "Already Existing Status : " + alreadyExisting);
        System.out.println(String.format("%1$tY%1$tm%1$td:%1$tH:%1$tM:%1$tS ", Calendar.getInstance(myExchangeObj.getExchangeTimeZone())) + "Finally returning (true is allow taking positions. false is do not allow) : " + returnValue);
        System.out.println(String.format("%1$tY%1$tm%1$td:%1$tH:%1$tM:%1$tS ", Calendar.getInstance(myExchangeObj.getExchangeTimeZone())) + " for trading contract " + tradingSignal.getTradingContractStructure() + " against monitoring contract " + tradingSignal.getMonitoringContractStructure() + " for Long/Short " + tradingSignal.getSideAndSize());

        return (returnValue);
    }

    boolean checkLastTradeTimeStamp(String closedPositionsQueueKeyName, String entryTimeStamp, String newSignalComboName) {

        boolean returnValue = true;
        String lastTradeTimeStamp = "20150000000000";
        Jedis jedis;

        // Go through all closed positions        
        jedis = jedisPool.getResource();

        try {
            // retrieve open position map from redis  
            Map<String, String> retrieveMap = jedis.hgetAll(closedPositionsQueueKeyName);
            for (String keyMap : retrieveMap.keySet()) {
                // Since position exists in closedQueue, check if exisitng pairPosition and new pair Position is same
                TradingObject myTradeObject = new TradingObject(retrieveMap.get(keyMap));
                if (myTradeObject.getTradingObjectName().matches(newSignalComboName)) {
                    // new position has been trades in past. Apply moratorium period
                    lastTradeTimeStamp = myTradeObject.getTradingContractExitTimeStamp();
                    // exit Time stamp of past trade occurs in the same day as new proposed entry
                    // make returnValue false if exit timestamp of existing position and now/new position is less than defined reentrydelay/moratorium i.e. one trading day equivalent
                    int elapsedTradingMinutes = 10 * myUtils.calcElapsedBars(jedisPool, lastTradeTimeStamp, entryTimeStamp, myExchangeObj, exchangeHolidayListKeyName, false);
                    if (elapsedTradingMinutes <= minimumMoratoriumForPosition) {
                        returnValue = false;
                    }
                    /*
                     //check if exit was due to loss. if loss then do something.
                     double roundTripBrokerage = Double.parseDouble(myTradeObject.getEntrySpread()) * (0.01 + 0.0019 + 0.0001) / 100;
                     if (myTradeObject.getEntryTimeStamp().substring(0,8).matches(myTradeObject.getExitTimeStamp().substring(0,8))) {
                     roundTripBrokerage = roundTripBrokerage + Double.parseDouble(myTradeObject.getExitSpread()) * (0.01 + 0.0019 + 0.0001) / 100;                    
                     } else {
                     roundTripBrokerage = roundTripBrokerage + Double.parseDouble(myTradeObject.getExitSpread()) * (0.01 + 0.0019 + 0.0001) / 100;                    
                     }
                     double tradePnL = 0;
                     if (Integer.parseInt(myTradeObject.getSideAndSize()) > 0) {
                     tradePnL += Double.parseDouble(myTradeObject.getExitSpread()) - Double.parseDouble(myTradeObject.getEntrySpread()) - roundTripBrokerage;
                     } else if (Integer.parseInt(myTradeObject.getSideAndSize()) < 0) {
                     tradePnL += Double.parseDouble(myTradeObject.getEntrySpread()) - Double.parseDouble(myTradeObject.getExitSpread()) - roundTripBrokerage;
                     }
                     if (tradePnL < 0) {
                     // DO SPECIFIC THINGS HERE IN CASE OF PREIOUS LOSS MAKING TRADE
                     } else {
                     // DO SPECIFIC THINGS HERE IN CASE OF PREVIOUS NON-NEGATIVE TRADE
                     }
                     */
                }
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

        // Debug Message
        System.out.println(String.format("%1$tY%1$tm%1$td:%1$tH:%1$tM:%1$tS ", Calendar.getInstance(myExchangeObj.getExchangeTimeZone())) + "lastTradeTimeStamp :" + lastTradeTimeStamp + " currentTimeStamp :" + entryTimeStamp + " returning : " + returnValue + " for " + newSignalComboName);

        return (returnValue);
    }

    boolean notBlackListed(String blacklistedSymbols, String signalSymbol) {
        boolean notFound = true;

        if ((blacklistedSymbols.length() > 1) && (signalSymbol.length() > 1)) {

            String symbolName = "";
            if (signalSymbol.split("_").length > 1) {
                symbolName = signalSymbol.split("_")[1];
            }
            String symbolsList[] = blacklistedSymbols.split(",");
            for (String symbol : symbolsList) {
                if (symbolName.equalsIgnoreCase(symbol)) {
                    notFound = false;
                }
            }
        }

        return (notFound);
    }

    boolean checkIfLongOrShortEntryAllowed(String allowLongIndicator, String allowShortIndicator, String side) {
        boolean returnValue = true;

        if ((allowLongIndicator.equalsIgnoreCase("yes")) && (allowShortIndicator.equalsIgnoreCase("yes"))) {
            returnValue = true;
        } else {
            if ((!(allowLongIndicator.equalsIgnoreCase("yes"))) && (Integer.parseInt(side) > 0)) {
                returnValue = false;
            }
            if ((!(allowShortIndicator.equalsIgnoreCase("yes"))) && (Integer.parseInt(side) < 0)) {
                returnValue = false;
            }
        }
        return (returnValue);
    }

    @Override
    public void run() {

        int firstEntryOrderTime = 940;
        String firstEntryOrderTimeConfigValue = myUtils.getHashMapValueFromRedis(jedisPool, redisConfigurationKey, "FIRSTENTRYORDERTIME", debugFlag);
        if ((firstEntryOrderTimeConfigValue != null) && (firstEntryOrderTimeConfigValue.length() > 0)) {
            firstEntryOrderTime = Integer.parseInt(firstEntryOrderTimeConfigValue);
        }

        myUtils.waitForStartTime(firstEntryOrderTime, myExchangeObj.getExchangeTimeZone(), "first entry order time", false);

        String entrySignalReceived = null;

        int eodExitTime = 1530;
        String eodExitTimeConfigValue = myUtils.getHashMapValueFromRedis(jedisPool, redisConfigurationKey, "EODEXITTIME", debugFlag);
        if ((eodExitTimeConfigValue != null) && (eodExitTimeConfigValue.length() > 0)) {
            eodExitTime = Integer.parseInt(eodExitTimeConfigValue);
        }

        // Enter an infinite loop with blocking pop call to retireve messages from queue
        // while market is open. Now start monitoring the open positions queue
        while (myUtils.marketIsOpen(eodExitTime, myExchangeObj.getExchangeTimeZone(), false)) {
            entrySignalReceived = myUtils.popKeyValueFromQueueRedis(jedisPool, entrySignalsQueueKeyName, 60, false);
            if (entrySignalReceived != null) {
                System.out.println(String.format("%1$tY%1$tm%1$td:%1$tH:%1$tM:%1$tS ", Calendar.getInstance(myExchangeObj.getExchangeTimeZone())) + "Info : Received Entry Signal as : " + entrySignalReceived);
                // Read the Maximum Possible Positions
                MAXLONGPOSITIONS = Integer.parseInt(myUtils.getHashMapValueFromRedis(jedisPool, redisConfigurationKey, "MAXNUMLONGPOSITIONS", false));
                MAXSHORTPOSITIONS = Integer.parseInt(myUtils.getHashMapValueFromRedis(jedisPool, redisConfigurationKey, "MAXNUMSHORTPOSITIONS", false));
                // Read the Maximun Number of Permissible Entries in a day including open positions at start of the day
                MAXNUMENTRIESINADAY = Integer.parseInt(myUtils.getHashMapValueFromRedis(jedisPool, redisConfigurationKey, "MAXNUMENTRIESINADAY", false));
                // Read the Days takeProfitLimit and stopLossLimit for no further new positions
                NOFURTHERPOSITIONTAKEPROFITLIMIT = Double.parseDouble(myUtils.getHashMapValueFromRedis(jedisPool, redisConfigurationKey, "NOFURTHERPOSITIONTAKEPROFITLIMIT", false));
                NOFURTHERPOSITIONSTOPLOSSLIMIT = Double.parseDouble(myUtils.getHashMapValueFromRedis(jedisPool, redisConfigurationKey, "NOFURTHERPOSITIONSTOPLOSSLIMIT", false));
                String blackListedSymbols = myUtils.getHashMapValueFromRedis(jedisPool, redisConfigurationKey, "BLACKLISTEDSYMBOLS", false);
                String allowLongIndicator = myUtils.getHashMapValueFromRedis(jedisPool, redisConfigurationKey, "ALLOWLONGENTRY", false);
                String allowShortIndicator = myUtils.getHashMapValueFromRedis(jedisPool, redisConfigurationKey, "ALLOWSHORTENTRY", false);

                String[] entrySignal = entrySignalReceived.split(",");
                int legSizeMultiple = Math.abs(Integer.parseInt(entrySignal[TradingObject.SIDE_SIZE_INDEX]));               
                // check if current time is within stipulated entry order time range and not stale by more than 5 minutes.
                // check if spread is not more than stipulated spread (say 200000 INR for NSE
                // check if the existing pair does not exist and there is space for Taking up the position, then enter position
                if ((nextOpenSlotNumber <= MAXNUMENTRIESINADAY)
                        && withinEntryOrderTimeRange(entrySignal[TradingObject.ENTRY_TIMESTAMP_INDEX])
                        && checkExistingOpenPositions(openPositionsQueueKeyName, entrySignalReceived)
                        && checkLastTradeTimeStamp(closedPositionsQueueKeyName, entrySignal[TradingObject.ENTRY_TIMESTAMP_INDEX], entrySignal[TradingObject.NAME_INDEX])
                        && notBlackListed(blackListedSymbols, entrySignal[TradingObject.NAME_INDEX])
                        && checkIfLongOrShortEntryAllowed(allowLongIndicator, allowShortIndicator, entrySignal[TradingObject.SIDE_SIZE_INDEX])
                        && withinStipulatedCurrentPnLForToday(MAXNUMENTRIESINADAY, openPositionsQueueKeyName, closedPositionsQueueKeyName, NOFURTHERPOSITIONTAKEPROFITLIMIT, NOFURTHERPOSITIONSTOPLOSSLIMIT)) {
                    // Block position slot - doing it here outside entry thread to avoid race condition which is seen happeneing if done inside thread
                    while ((myUtils.checkIfExistsHashMapField(jedisPool, openPositionsQueueKeyName, Integer.toString(nextOpenSlotNumber), false))) {
                        nextOpenSlotNumber++;
                    }
                    blockOpenPositionSlot(openPositionsQueueKeyName, entrySignalReceived, nextOpenSlotNumber);
                    // Read the order type
                    confOrderType = myUtils.getHashMapValueFromRedis(jedisPool, redisConfigurationKey, "ENTRYORDERTYPE", false);
                    // Read the stop loss amount to use
                    int initialStopLoss = 10000;
                    if (myUtils.checkIfExistsHashMapField(jedisPool, redisConfigurationKey, "INITIALSTOPLOSSTYPE", false)) {
                        String initialStopLossType = myUtils.getHashMapValueFromRedis(jedisPool, redisConfigurationKey, "INITIALSTOPLOSSTYPE", false);
                        if (initialStopLossType.equalsIgnoreCase("fixedamount")) {
                            initialStopLoss = legSizeMultiple * Integer.parseInt(myUtils.getHashMapValueFromRedis(jedisPool, redisConfigurationKey, "INITIALSTOPLOSSAMOUNT", false));
                        } else if (initialStopLossType.equalsIgnoreCase("sigmafactor")) {
                            double stopLossFactor = Double.parseDouble(myUtils.getHashMapValueFromRedis(jedisPool, redisConfigurationKey, "INITIALSTOPLOSSSIGMAFACTOR", false));
                            double oneSigmaAmount = Double.parseDouble(entrySignal[TradingObject.MONITORING_CONTRACT_ENTRY_STDDEV_INDEX]);
                            if (oneSigmaAmount <= 0) {
                                oneSigmaAmount = 3000;
                            }
                            initialStopLoss = Math.abs((int) Math.round(legSizeMultiple * stopLossFactor * oneSigmaAmount));
                        }
                    }
                    // Enter the order
                    SingleLegEntry newPositionEntry = new SingleLegEntry("TakingNewPositionThread", entrySignalReceived, jedisPool, ibInteractionClient, myUtils, strategyName, openPositionsQueueKeyName, myExchangeObj.getExchangeTimeZone(), confOrderType, nextOpenSlotNumber, initialStopLoss, myExchangeObj.getExchangeCurrency(), true);
                    newPositionEntry.start();
                    // Increment the next Open slotNumber
                    nextOpenSlotNumber++;
                }
            }
        }
        // Day Over. Now Exiting.
    }

    @Override
    public void start() {
        this.setName(threadName);
        if (t == null) {
            t = new Thread(this, threadName);
            t.start();
        }
    }

}
