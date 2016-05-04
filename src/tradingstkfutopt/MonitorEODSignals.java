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

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import redis.clients.jedis.*;
import redis.clients.jedis.exceptions.JedisException;

/**
 * @author Manish Kumar Singh
 */
public class MonitorEODSignals extends Thread {

    private Thread t;
    private String threadName = "MonitoringEndOfDayEntryExitSignalsThread";
    private boolean debugFlag;
    private JedisPool jedisPool;

    private String redisConfigurationKey;

    private IBInteraction ibInteractionClient;
    private MyExchangeClass myExchangeObj;

    private MyUtils myUtils;

    private String strategyName = "singlestr01";
    private String openPositionsQueueKeyName = "INRSTR01OPENPOSITIONS";
    private String entrySignalsQueueKeyName = "INRSTR01ENTRYSIGNALS";  
    private String manualInterventionSignalsQueueKeyName = "INRSTR01MANUALINTERVENTIONS";    
    private String eodEntryExitSignalsQueueKeyName = "INRSTR01EODENTRYEXITSIGNALS";

    public String exchangeHolidayListKeyName;
    
    public class MyExistingPositionClass {
        String symbolName, positionSymbolType; // symbolType would be "STK" or "FUT" or "OPT"
        String optionRightType = "NA";
        int timeSlot;
        int positionSideAndSize;
        String monitoringSymbolType; // symbolType would be "IND" or "STK" or "FUT" or "OPT"
        
        public MyExistingPositionClass(int timeSlot) {
            this.symbolName = "NA";
            this.positionSymbolType = "FUT";
            this.optionRightType = "NA";
            this.timeSlot = timeSlot;
            this.positionSideAndSize = 0;
            this.monitoringSymbolType = "IND";
        }        
    }

    public ConcurrentHashMap<Integer, MyExistingPositionClass> myExistingPositionMap = new ConcurrentHashMap<Integer, MyExistingPositionClass>();

    public class MyTimeSlotSubscriptionsClass {
        String tradingContractType; // Type would be "STK" or "FUT" or "OPT"
        int legSizeMultiple;
        String monitoringContractType; // Type would be "IND" or "STK" or "FUT" or "OPT"
        String brokerToUse; //Currently this would support "IB" or "ZERODHA"

        public MyTimeSlotSubscriptionsClass(int legSize) {
            this.tradingContractType = "FUT";
            this.monitoringContractType = "IND";
            this.legSizeMultiple = legSize;
            this.brokerToUse = "IB";
        }
    }
    //subscription - structure contracttotrade:contracttomonitor:legsizemultiple:broker
    //e.g. 1015:OPT:IND:2:ZERODHA
    //means subscribed for 10:15 timeslot with options to trade but index to monitor and legsize is twice of lotsize with 
    //broker as ZERODHA
    //contract to trade could be OPT or FUT or STK. contract to monitor could be IND which is INDEX or STK or OPT or FUT. 
    //broker could be IB or ZERODHA

    public ConcurrentHashMap<Integer, MyTimeSlotSubscriptionsClass> myTimeSlotSubscriptionsMap = new ConcurrentHashMap<Integer, MyTimeSlotSubscriptionsClass>();
    
    public class MyEntrySignalParameters {
        String elementName, elementStructure, signalTimeStamp, signalType;
        int tradeSide, elementLotSize, timeSlot, RLSignal, TSISignal;
        double onePctReturn, spread;
        
        // Structure of signal is SYMBOLNAME,LOTSIZE,RLSIGNAL,TSISIGNAL,HALFLIFE,CURRENTSTATE,ZSCORE,
        //    DTSMA200,ONEPCTRETURN,QSCORE,SPREAD,STRUCTURE,TIMESTAMP,SIGNALTYPE,TIMESLOT

        // Index Values for Object
        final int EODSIGNALSYMBOLNAME_INDEX = 0;
        final int EODSIGNALLOTSIZE_INDEX = 1;
        final int EODSIGNALRLSIGNAL_INDEX = 2;
        final int EODSIGNALTSISIGNAL_INDEX = 3;
        final int EODSIGNALONEPCTRETURN_INDEX = 4;
        final int EODSIGNALSPREAD_INDEX = 5;
        final int EODSIGNALSTRUCTURE_INDEX = 6;
        final int EODSIGNALTIMESTAMP_INDEX = 7;
        final int EODSIGNALSIGNALTYPE_INDEX = 8;
        final int EODSIGNALTIMESLOT_INDEX = 9;
        
        final int EODSIGNALMAX_ELEMENTS = 10;
        String[] eodSignalObjectStructure;        
        
        MyEntrySignalParameters(String incomingSignal) {
            eodSignalObjectStructure = new String[EODSIGNALMAX_ELEMENTS];

            if (incomingSignal.length() > 0) {
                String[] tempObjectStructure = incomingSignal.split(",");
                for (int index = 0; (index < tempObjectStructure.length) && (index < EODSIGNALMAX_ELEMENTS); index++) {
                        eodSignalObjectStructure[index] = tempObjectStructure[index];
                }
            }
            
            this.elementName = eodSignalObjectStructure[EODSIGNALSYMBOLNAME_INDEX];
            this.elementLotSize = Integer.parseInt(eodSignalObjectStructure[EODSIGNALLOTSIZE_INDEX]);
            this.RLSignal = Integer.parseInt(eodSignalObjectStructure[EODSIGNALRLSIGNAL_INDEX]);
            this.TSISignal = Integer.parseInt(eodSignalObjectStructure[EODSIGNALTSISIGNAL_INDEX]);            
            this.tradeSide = this.RLSignal;
            this.onePctReturn = Double.parseDouble(eodSignalObjectStructure[EODSIGNALONEPCTRETURN_INDEX]);
            this.spread = Double.parseDouble(eodSignalObjectStructure[EODSIGNALSPREAD_INDEX]);
            this.elementStructure = eodSignalObjectStructure[EODSIGNALSTRUCTURE_INDEX];
            this.signalTimeStamp = eodSignalObjectStructure[EODSIGNALTIMESTAMP_INDEX];           
            this.signalType = eodSignalObjectStructure[EODSIGNALSIGNALTYPE_INDEX];
            this.timeSlot = Integer.parseInt(eodSignalObjectStructure[EODSIGNALTIMESLOT_INDEX]);            
        } 
    }
    
    MonitorEODSignals(String name, JedisPool redisConnectionPool, String redisConfigKey, MyUtils utils, MyExchangeClass exchangeObj, IBInteraction ibInterClient, boolean debugIndicator) {

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
        entrySignalsQueueKeyName = myUtils.getHashMapValueFromRedis(jedisPool, redisConfigurationKey, "ENTRYSIGNALSQUEUE", false);
        manualInterventionSignalsQueueKeyName = myUtils.getHashMapValueFromRedis(jedisPool, redisConfigurationKey, "MANUALINTERVENTIONQUEUE", false);        
        eodEntryExitSignalsQueueKeyName = myUtils.getHashMapValueFromRedis(jedisPool, redisConfigurationKey, "EODENTRYEXITSIGNALSQUEUE", false);

        exchangeHolidayListKeyName = myUtils.getHashMapValueFromRedis(jedisPool, redisConfigurationKey, "EXCHANGEHOLIDAYLISTKEYNAME", false);

        // Debug Message
        System.out.println(String.format("%1$tY%1$tm%1$td:%1$tH:%1$tM:%1$tS ", Calendar.getInstance(myExchangeObj.getExchangeTimeZone())) + "Info : Monitoring End of Day Entry/Exit Signals for Strategy " + strategyName + " EOD queue Name " + eodEntryExitSignalsQueueKeyName);
    }

    int getNearestHundredStrikePrice(double indexLevel) {
        int returnValue = -1;
        
        returnValue = 100 * (int) (Math.round(indexLevel + 50) / 100);
        
        return(returnValue);
    }

    int getTimeSlot(String YYYYMMDDHHMMSS) {

        int retValue = 0;

        String HHMM = YYYYMMDDHHMMSS.substring(8, 10);
        int MM = Integer.parseInt(YYYYMMDDHHMMSS.substring(10, 12));
        if ((MM >= 0) && (MM < 15)) {
            HHMM = HHMM + "00";
        } else if ((MM >= 15) && (MM < 30)) {
            HHMM = HHMM + "15";
        } else if ((MM >= 30) && (MM < 45)) {
            HHMM = HHMM + "30";
        } else if (MM >= 45) {
            HHMM = HHMM + "45";
        }                    

        retValue = Integer.parseInt(HHMM);
        
        return (retValue);
    } // End of method
    
    int getNumPositionsTimeSlot(String symbolName, int timeSlot) {
        // Find number of positions in current timeSlot.
        int numPositionsCurrentTimeSlot = 0;
        for ( Integer currentSlotNum : myExistingPositionMap.keySet()) {
            if ( (myExistingPositionMap.get(currentSlotNum).timeSlot == timeSlot) && 
                    myExistingPositionMap.get(currentSlotNum).symbolName.equalsIgnoreCase(symbolName) ) {
                numPositionsCurrentTimeSlot++;
            }
        }
        return(numPositionsCurrentTimeSlot);
    }
    
    void updateCurrentTimeSlotSubscriptions() {

        String[] subscribedTimeSlotList = myUtils.getHashMapValueFromRedis(jedisPool, redisConfigurationKey, "SUBSCRIBEDTIMESLOTS", false).split(",");
        myTimeSlotSubscriptionsMap.clear();
        for(String timeSlotSubscription : subscribedTimeSlotList) {
            String[] timeSlotDetails = timeSlotSubscription.split(":");
            //subscription - structure contracttotrade:contracttomonitor:legsizemultiple:broker
            //e.g. 1015:OPT:IND:2:ZERODHA            
            int timeSlotHHMM = Integer.parseInt(timeSlotDetails[0]);
            MyTimeSlotSubscriptionsClass localTimeSlotSubscriptionObj = new MyTimeSlotSubscriptionsClass(1);
            localTimeSlotSubscriptionObj.tradingContractType = timeSlotDetails[1];
            localTimeSlotSubscriptionObj.monitoringContractType = timeSlotDetails[2];
            localTimeSlotSubscriptionObj.legSizeMultiple = Integer.parseInt(timeSlotDetails[3]);
            localTimeSlotSubscriptionObj.brokerToUse = timeSlotDetails[4];                        
            myTimeSlotSubscriptionsMap.put(timeSlotHHMM, localTimeSlotSubscriptionObj);
        }        
    } // End of Method
     
    void getExistingPositionDetailsForAllPositions(String openPositionsQueueKeyName) {

        myExistingPositionMap.clear();
        
        Jedis jedis;
        jedis = jedisPool.getResource();
        try {
            // retrieve open position map from redis  
            Map<String, String> retrieveMap = jedis.hgetAll(openPositionsQueueKeyName);
            // Go through all open position slots to check for existance of Current Symbol        
            for (String keyMap : retrieveMap.keySet()) {
                // Do Stuff here             
                TradingObject myTradeObject = new TradingObject(retrieveMap.get(keyMap));
                int positionTimeSlot = getTimeSlot(myTradeObject.getEntryTimeStamp());
                if ( (myTradeObject.getOrderState().equalsIgnoreCase("entryorderfilled")) ) {
                    //set the values of parameters...
                    int slotNumber = Integer.parseInt(keyMap);
                    MyExistingPositionClass tempExistingPositionObj = new MyExistingPositionClass(positionTimeSlot);
                    tempExistingPositionObj.symbolName = myTradeObject.getTradingObjectName();
                    tempExistingPositionObj.timeSlot = positionTimeSlot;
                    tempExistingPositionObj.positionSideAndSize = myTradeObject.getSideAndSize();
                    tempExistingPositionObj.positionSymbolType = myTradeObject.getTradingContractType();                                            
                    if (myTradeObject.getTradingContractType().equalsIgnoreCase("OPT")) {
                        tempExistingPositionObj.optionRightType = myTradeObject.getTradingContractOptionRightType();
                    }
                    tempExistingPositionObj.monitoringSymbolType = myTradeObject.getTradingContractType();
                    if (myTimeSlotSubscriptionsMap.containsKey(positionTimeSlot)){
                        tempExistingPositionObj.monitoringSymbolType = myTimeSlotSubscriptionsMap.get(positionTimeSlot).monitoringContractType;
                    }                    
                    myExistingPositionMap.put(slotNumber, tempExistingPositionObj);
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
    } // end of Method    
     
    void getExistingPositionDetailsForAllTimeSlots(String openPositionsQueueKeyName, String signalSymbolName) {

        Jedis jedis;
        jedis = jedisPool.getResource();
        try {
            // retrieve open position map from redis  
            Map<String, String> retrieveMap = jedis.hgetAll(openPositionsQueueKeyName);
            // Go through all open position slots to check for existance of Current Symbol        
            for (String keyMap : retrieveMap.keySet()) {
                // Do Stuff here             
                TradingObject myTradeObject = new TradingObject(retrieveMap.get(keyMap));
                int positionTimeSlot = getTimeSlot(myTradeObject.getEntryTimeStamp());
                if ( (myTradeObject.getTradingObjectName().matches("ON_" + signalSymbolName)) &&
                        (myTradeObject.getOrderState().equalsIgnoreCase("entryorderfilled")) ) {
                    // if position belongs to timeslot and name matches the 
                    //set the values of parameters...
                    int slotNumber = Integer.parseInt(keyMap);
                    if (myExistingPositionMap.containsKey(slotNumber)) {
                        // details exists from previous entries. Update it
                        myExistingPositionMap.get(slotNumber).timeSlot = positionTimeSlot;
                        myExistingPositionMap.get(slotNumber).positionSideAndSize = myTradeObject.getSideAndSize();
                        myExistingPositionMap.get(slotNumber).positionSymbolType = myTradeObject.getTradingContractType();                                            
                        if (myTradeObject.getTradingContractType().equalsIgnoreCase("OPT")) {
                            myExistingPositionMap.get(slotNumber).optionRightType = myTradeObject.getTradingContractOptionRightType();
                        }
                        myExistingPositionMap.get(slotNumber).monitoringSymbolType = myTradeObject.getTradingContractType();
                        if (myTimeSlotSubscriptionsMap.containsKey(positionTimeSlot)){
                            myExistingPositionMap.get(slotNumber).monitoringSymbolType = myTimeSlotSubscriptionsMap.get(positionTimeSlot).monitoringContractType;
                        }                        
                    } else {
                        // no previous entry exists for given slot number so create one
                        MyExistingPositionClass tempExistingPositionObject = new MyExistingPositionClass(positionTimeSlot);
                        tempExistingPositionObject.symbolName = myTradeObject.getTradingObjectName();
                        tempExistingPositionObject.timeSlot = positionTimeSlot;
                        tempExistingPositionObject.positionSideAndSize = myTradeObject.getSideAndSize();
                        tempExistingPositionObject.positionSymbolType = myTradeObject.getTradingContractType();                                            
                        if (myTradeObject.getTradingContractType().equalsIgnoreCase("OPT")) {
                            tempExistingPositionObject.optionRightType = myTradeObject.getTradingContractOptionRightType();
                        }
                        tempExistingPositionObject.monitoringSymbolType = myTradeObject.getTradingContractType();
                        if (myTimeSlotSubscriptionsMap.containsKey(positionTimeSlot)){
                            tempExistingPositionObject.monitoringSymbolType = myTimeSlotSubscriptionsMap.get(positionTimeSlot).monitoringContractType;
                        }                                                
                        myExistingPositionMap.put(slotNumber, tempExistingPositionObject);
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
    } // end of Method    
    
    void getExistingPositionDetailsForGivenTimeSlot(String openPositionsQueueKeyName, String signalSymbolName, int signalTimeSlot) {
                
        Jedis jedis;
        jedis = jedisPool.getResource();
        try {
            // retrieve open position map from redis  
            Map<String, String> retrieveMap = jedis.hgetAll(openPositionsQueueKeyName);
            // Go through all open position slots to check for existance of Current Symbol        
            for (String keyMap : retrieveMap.keySet()) {
                // Do Stuff here             
                TradingObject myTradeObject = new TradingObject(retrieveMap.get(keyMap));
                int positionTimeSlot = getTimeSlot(myTradeObject.getEntryTimeStamp());
                if (( signalTimeSlot == positionTimeSlot ) && 
                        (myTradeObject.getTradingObjectName().matches("ON_" + signalSymbolName)) &&
                        (myTradeObject.getOrderState().equalsIgnoreCase("entryorderfilled")) ) {
                    // if position belongs to timeslot and name matches the 
                    //set the values of parameters...
                    int slotNumber = Integer.parseInt(keyMap);
                    if (myExistingPositionMap.containsKey(slotNumber)) {
                        // details exists from previous entries. Update it
                        myExistingPositionMap.get(slotNumber).timeSlot = positionTimeSlot;
                        myExistingPositionMap.get(slotNumber).positionSideAndSize = myTradeObject.getSideAndSize();
                        myExistingPositionMap.get(slotNumber).positionSymbolType = myTradeObject.getTradingContractType();                                            
                        if (myTradeObject.getTradingContractType().equalsIgnoreCase("OPT")) {
                            myExistingPositionMap.get(slotNumber).optionRightType = myTradeObject.getTradingContractOptionRightType();
                        }
                        myExistingPositionMap.get(slotNumber).monitoringSymbolType = myTradeObject.getTradingContractType();
                        if (myTimeSlotSubscriptionsMap.containsKey(positionTimeSlot)){
                            myExistingPositionMap.get(slotNumber).monitoringSymbolType = myTimeSlotSubscriptionsMap.get(positionTimeSlot).monitoringContractType;
                        }                        
                    } else {
                        // no previous entry exists for given slot number so create one
                        MyExistingPositionClass tempExistingPositionObj = new MyExistingPositionClass(positionTimeSlot);
                        tempExistingPositionObj.symbolName = myTradeObject.getTradingObjectName();
                        tempExistingPositionObj.timeSlot = positionTimeSlot;
                        tempExistingPositionObj.positionSideAndSize = myTradeObject.getSideAndSize();
                        tempExistingPositionObj.positionSymbolType = myTradeObject.getTradingContractType();                                            
                        if (myTradeObject.getTradingContractType().equalsIgnoreCase("OPT")) {
                            tempExistingPositionObj.optionRightType = myTradeObject.getTradingContractOptionRightType();
                        }
                        tempExistingPositionObj.monitoringSymbolType = myTradeObject.getTradingContractType();
                        if (myTimeSlotSubscriptionsMap.containsKey(positionTimeSlot)){
                            tempExistingPositionObj.monitoringSymbolType = myTimeSlotSubscriptionsMap.get(positionTimeSlot).monitoringContractType;
                        }                        
                        myExistingPositionMap.put(slotNumber, tempExistingPositionObj);                        
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

    }    
    
    void sendUpdateStoplossLimitSignal(String manualInterventionSignalsQueue, int slotNumber) {
        // perl code to push stop loss signal

        Calendar timeNow = Calendar.getInstance(myExchangeObj.getExchangeTimeZone());        
        String stopLossSignal = String.format("%1$tY%1$tm%1$td%1$tH%1$tM%1$tS", timeNow) + ",trade," + slotNumber + ",1,0";

        Jedis jedis;
        jedis = jedisPool.getResource();
        try {
            // push the square off signal
//            jedis.lpush(manualInterventionSignalsQueue, stopLossSignal);
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
    
    void sendSquareOffSignal(String manualInterventionSignalsQueue, int slotNumber) {
        // perl code to push square off signal
        //$MISignalForRedis = $YYYYMMDDHHMMSS.",trade,".$positionID.",1,manualIntervention,0";
        //$redis->lpush($queueKeyName, $MISignalForRedis);

        Calendar timeNow = Calendar.getInstance(myExchangeObj.getExchangeTimeZone());        
        String squareOffSignal = String.format("%1$tY%1$tm%1$td%1$tH%1$tM%1$tS", timeNow) + ",trade," + slotNumber + ",1,exitSignal,0";

        Jedis jedis;
        jedis = jedisPool.getResource();
        try {
            // push the square off signal
            jedis.lpush(manualInterventionSignalsQueue, squareOffSignal);
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
    
    void sendEntrySignal(String entrySignalsQueue, MyEntrySignalParameters signalParam) {
        // R code to form entry signal
        //entrySignalForRedis <- paste0(timestamp,",","ON_",elementName,",",tradeSide,",",elementName,"_",elementLotSize,","
        //,zscore[1],",",t_10min$dtsma_200[lastIndex],",",halflife,",",onePercentReturn,",",positionRank,",",abs(predSVM),
        //",1,",t_10min$spread[lastIndex]);

        // Check for subscription for given timeslot. Proceed only if subscribed
        if (!myTimeSlotSubscriptionsMap.containsKey(signalParam.timeSlot)) {
            return;
        }
        
        String entrySignal = null;
        String tradingContractType = myTimeSlotSubscriptionsMap.get(signalParam.timeSlot).tradingContractType;
        int legSizeMultiple = myTimeSlotSubscriptionsMap.get(signalParam.timeSlot).legSizeMultiple;
        int tradeSideAndSize = signalParam.tradeSide * legSizeMultiple;
        String tradeContractStructure = signalParam.elementName + "_" + signalParam.elementLotSize + "_" + tradingContractType;
        String monitoringContractType = myTimeSlotSubscriptionsMap.get(signalParam.timeSlot).monitoringContractType;
        String monitorContractStructure = signalParam.elementName + "_" + monitoringContractType;        

        //subscription - structure contracttotrade:contracttomonitor:legsizemultiple:broker
        //e.g. 1015:OPT:IND:2:ZERODHA        
        String timeSlotSubscription = signalParam.timeSlot 
                + ":" + myTimeSlotSubscriptionsMap.get(signalParam.timeSlot).tradingContractType
                + ":" + myTimeSlotSubscriptionsMap.get(signalParam.timeSlot).monitoringContractType
                + ":" + myTimeSlotSubscriptionsMap.get(signalParam.timeSlot).legSizeMultiple
                + ":" + myTimeSlotSubscriptionsMap.get(signalParam.timeSlot).brokerToUse;
        
        if (tradingContractType.equalsIgnoreCase("OPT")) {
            // this is for OPT type of contract
            String optionRightType = "CALL";
            int optionStrikePrice = -1;
            if (signalParam.tradeSide > 0) {
                optionRightType = "CALL";
                tradeSideAndSize = 1 * signalParam.tradeSide;               
            } else if (signalParam.tradeSide < 0) {
                optionRightType = "PUT";
                tradeSideAndSize = -1 * signalParam.tradeSide;
            }
            optionStrikePrice = getNearestHundredStrikePrice(signalParam.spread/signalParam.elementLotSize);
            tradeContractStructure = signalParam.elementName + "_" + signalParam.elementLotSize + "_" + "OPT" + "_" + optionRightType + "_" + optionStrikePrice;
        }
            
        entrySignal = signalParam.signalTimeStamp + "," 
        + "ON_" + signalParam.elementName + "," 
        + tradeSideAndSize + "," 
        + tradeContractStructure + ","
        + String.format( "%.2f", signalParam.onePctReturn ) + "," 
        + monitorContractStructure + ","
        + String.format( "%.2f", signalParam.onePctReturn ) + ","
        + timeSlotSubscription + ","                
        + "signalsent";

        if (entrySignal != null) {
            Jedis jedis;
            jedis = jedisPool.getResource();
            try {
                // push the entry signal
                jedis.lpush(entrySignalsQueue, entrySignal);              
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
        
    }
    
    void processEntryExitSignal(MyEntrySignalParameters signalParam) {
        // RLSIGNAL - Reinforced Learning Signal - can take values 0, +1, -1 
        // 0 meaning non action necessary
        // +1 meaning recommended Buy if no position/Square off if existing Short position
        // -1 meaning recommended Sell if no position/Square off if existing Long position
        // TSISIGNAL - True Strength Index Signal - can take values 0, +1, -1 
        // 0 meaning non action necessary
        // +1 meaning recommended Square off if existing Short position
        // -1 meaning recommended Square off if existing Long position

        // trading rule implemented :
        // if no position for given symbol then take position as per RLSIGNAL
        // else if LONG position exists for given symbol then square off if either of RLSIGNAL OR TSISIGNAL are -1
        // else if SHORT position exists for given symbol then square off if either of RLSIGNAL OR TSISIGNAL are +1

        // for taking position, send form the signal and send to entrySignalsQueue
        // for square off, send signal to manual intervention queue

        //If no positions exist then take position
        if (getNumPositionsTimeSlot(signalParam.elementName, signalParam.timeSlot) <= 0) {
            // since no position for given symbol so take position as per RLSIGNAL
            if (signalParam.tradeSide != 0) {
                sendEntrySignal(entrySignalsQueueKeyName, signalParam);                        
            }
        // if position exist for signal time stamp, then process it
        } else {
            for ( Integer currentSlotNum : myExistingPositionMap.keySet()) {
                if ((myExistingPositionMap.get(currentSlotNum).timeSlot == signalParam.timeSlot) &&
                        myExistingPositionMap.get(currentSlotNum).symbolName.equalsIgnoreCase(signalParam.elementName) &&
                        (myExistingPositionMap.get(currentSlotNum).positionSymbolType.equalsIgnoreCase("FUT") ||
                        myExistingPositionMap.get(currentSlotNum).positionSymbolType.equalsIgnoreCase("STK")) ){
                    //if LONG position exists for given symbol then square off if either of RLSIGNAL OR TSISIGNAL are -1
                    if (myExistingPositionMap.get(currentSlotNum).positionSideAndSize > 0) {
                        if ( (signalParam.RLSignal < 0 ) || (signalParam.TSISignal < 0) ) {
                            sendSquareOffSignal(manualInterventionSignalsQueueKeyName,currentSlotNum);
                            // if exit is due to RLSignal, then take opposite position after exiting
                            if ( (signalParam.RLSignal < 0 ) && (signalParam.tradeSide != 0)) {
                                sendEntrySignal(entrySignalsQueueKeyName, signalParam);                        
                            }                                                        
                        }
                    }
                    //if SHORT position exists for given symbol then square off if either of RLSIGNAL OR TSISIGNAL are +1                        
                    if (myExistingPositionMap.get(currentSlotNum).positionSideAndSize < 0 ) {
                        if ( (signalParam.RLSignal > 0 ) || (signalParam.TSISignal > 0) ) {
                            sendSquareOffSignal(manualInterventionSignalsQueueKeyName,currentSlotNum);
                            // if exit is due to RLSignal, then take do opposite position after exiting
                            if ( (signalParam.RLSignal > 0 ) && (signalParam.tradeSide != 0)) {
                                sendEntrySignal(entrySignalsQueueKeyName, signalParam);                        
                            }                                                                                    
                        }                                                
                    }
                } else if ((myExistingPositionMap.get(currentSlotNum).timeSlot == signalParam.timeSlot) &&
                        myExistingPositionMap.get(currentSlotNum).symbolName.equalsIgnoreCase(signalParam.elementName) &&                        
                        myExistingPositionMap.get(currentSlotNum).positionSymbolType.equalsIgnoreCase("OPT") ){
                    //if LONG position exists i.e. it is CALL OPTION then square off if either of RLSIGNAL OR TSISIGNAL are -1
                    if (myExistingPositionMap.get(currentSlotNum).optionRightType.equalsIgnoreCase("CALL") || 
                            myExistingPositionMap.get(currentSlotNum).optionRightType.equalsIgnoreCase("C") ) {
                        if ( (signalParam.RLSignal < 0 ) || (signalParam.TSISignal < 0) ) {
                            sendSquareOffSignal(manualInterventionSignalsQueueKeyName,currentSlotNum);
                            // if exit is due to RLSignal, then take opposite position after exiting
                            if ( (signalParam.RLSignal < 0 ) && (signalParam.tradeSide != 0)) {
                                sendEntrySignal(entrySignalsQueueKeyName, signalParam);                        
                            }                                                        
                        }
                    }
                    //if SHORT position exists i.e. it is PUT OPTION then square off if either of RLSIGNAL OR TSISIGNAL are +1                        
                    if (myExistingPositionMap.get(currentSlotNum).optionRightType.equalsIgnoreCase("PUT") || 
                            myExistingPositionMap.get(currentSlotNum).optionRightType.equalsIgnoreCase("P") ) {
                        if ( (signalParam.RLSignal > 0 ) || (signalParam.TSISignal > 0) ) {
                            sendSquareOffSignal(manualInterventionSignalsQueueKeyName,currentSlotNum);
                            // if exit is due to RLSignal, then take do opposite position after exiting
                            if ( (signalParam.RLSignal > 0 ) && (signalParam.tradeSide != 0)) {
                                sendEntrySignal(entrySignalsQueueKeyName, signalParam);                        
                            }                                                                                    
                        }                                                
                    }                
                }                
            }                        
        }
    } // End of Method


    void processExitSignalForMatchingPosition(MyEntrySignalParameters signalParam) {
        // symbolName, timeSlot and signalType are important. Other signal fields are redundant
        if (getNumPositionsTimeSlot(signalParam.elementName, signalParam.timeSlot) > 0) {
            // if position exist for signal time stamp, then process it            
            for ( Integer currentSlotNum : myExistingPositionMap.keySet()) {
               if ((myExistingPositionMap.get(currentSlotNum).timeSlot == signalParam.timeSlot) &&
                        myExistingPositionMap.get(currentSlotNum).symbolName.equalsIgnoreCase(signalParam.elementName) ){
                    sendSquareOffSignal(manualInterventionSignalsQueueKeyName,currentSlotNum);                
                }
            }
        }                        
    }
    
    void processExitSignalForAllPositions(MyEntrySignalParameters signalParam) {
        // trading rule implemented :
        // square off all positions
        // for square off, send signal to manual intervention queue
        Jedis jedis;
        jedis = jedisPool.getResource();
        try {
            // retrieve open position map from redis  
            Map<String, String> retrieveMap = jedis.hgetAll(openPositionsQueueKeyName);
            // Go through all open position slots to check for existance of Current Symbol        
            for (String keyMap : retrieveMap.keySet()) {
                // Do Stuff here             
                TradingObject myTradeObject = new TradingObject(retrieveMap.get(keyMap));
                if ( myTradeObject.getOrderState().equalsIgnoreCase("entryorderfilled") ) {
                    // position exists and suitable for square off
                    sendSquareOffSignal(manualInterventionSignalsQueueKeyName,Integer.parseInt(keyMap));                    
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

    }    

    void processUpdateStoplossLimitForMatchingPosition(MyEntrySignalParameters signalParam) {
        // symbolName, timeSlot and signalType are important. Other signal fields are redundant
        if (getNumPositionsTimeSlot(signalParam.elementName, signalParam.timeSlot) > 0) {
            // if position exist for signal time stamp, then process it            
            for ( Integer currentSlotNum : myExistingPositionMap.keySet()) {
               if ((myExistingPositionMap.get(currentSlotNum).timeSlot == signalParam.timeSlot) &&
                        myExistingPositionMap.get(currentSlotNum).symbolName.equalsIgnoreCase(signalParam.elementName) ){
                    sendUpdateStoplossLimitSignal(manualInterventionSignalsQueueKeyName,currentSlotNum);                
                }
            }
        }                                
    }
    
    void processUpdateStoplossLimitForAllPositions(MyEntrySignalParameters signalParam) {
        // symbolName and signalType are important. Other signal fields are redundant
        // trading rule implemented :
        // square off all positions
        // for square off, send signal to manual intervention queue
        Jedis jedis;
        jedis = jedisPool.getResource();
        try {
            // retrieve open position map from redis  
            Map<String, String> retrieveMap = jedis.hgetAll(openPositionsQueueKeyName);
            // Go through all open position slots to check for existance of Current Symbol        
            for (String keyMap : retrieveMap.keySet()) {
                // Do Stuff here             
                TradingObject myTradeObject = new TradingObject(retrieveMap.get(keyMap));
                if ( myTradeObject.getOrderState().equalsIgnoreCase("entryorderfilled") ) {
                    // position exists and suitable for square off
                    sendUpdateStoplossLimitSignal(manualInterventionSignalsQueueKeyName,Integer.parseInt(keyMap));                    
                    // Debug Messages if any
                    System.out.println(String.format("%1$tY%1$tm%1$td:%1$tH:%1$tM:%1$tS ", Calendar.getInstance(myExchangeObj.getExchangeTimeZone())) + " Updating Stop Loss for " + keyMap + " for symbol " + signalParam.elementName);
                }
                // Debug Messages if any
                System.out.println(String.format("%1$tY%1$tm%1$td:%1$tH:%1$tM:%1$tS ", Calendar.getInstance(myExchangeObj.getExchangeTimeZone())) + " Processed Stop Loss Signal " + keyMap + " for symbol " + signalParam.elementName);                
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

    }        
    
    @Override
    public void run() {

        int firstEntryOrderTime = 1515;
        String firstEntryOrderTimeConfigValue = myUtils.getHashMapValueFromRedis(jedisPool, redisConfigurationKey, "FIRSTENTRYORDERTIME", debugFlag);
        if ((firstEntryOrderTimeConfigValue != null) && (firstEntryOrderTimeConfigValue.length() > 0)) {
            firstEntryOrderTime = Integer.parseInt(firstEntryOrderTimeConfigValue);
        }

        myUtils.waitForStartTime(firstEntryOrderTime, myExchangeObj.getExchangeTimeZone(), "first entry order time", false);

        String eodSignalReceived = null;

        int eodExitTime = 1530;
        String eodExitTimeConfigValue = myUtils.getHashMapValueFromRedis(jedisPool, redisConfigurationKey, "EODEXITTIME", debugFlag);
        if ((eodExitTimeConfigValue != null) && (eodExitTimeConfigValue.length() > 0)) {
            eodExitTime = Integer.parseInt(eodExitTimeConfigValue);
        }

        // Enter an infinite loop with blocking pop call to retireve messages from queue
        // while market is open keep monitoring eod signals queue
        while (myUtils.marketIsOpen(eodExitTime, myExchangeObj.getExchangeTimeZone(), false)) {
            eodSignalReceived = myUtils.popKeyValueFromQueueRedis(jedisPool, eodEntryExitSignalsQueueKeyName, 60, false);
            if (eodSignalReceived != null) {
                System.out.println(String.format("%1$tY%1$tm%1$td:%1$tH:%1$tM:%1$tS ", Calendar.getInstance(myExchangeObj.getExchangeTimeZone())) + "Info : Received EOD Signal as : " + eodSignalReceived);                
                updateCurrentTimeSlotSubscriptions();
                getExistingPositionDetailsForAllPositions(openPositionsQueueKeyName);

                MyEntrySignalParameters signalParam = new MyEntrySignalParameters(eodSignalReceived);
                // Structure of signal is SYMBOLNAME,LOTSIZE,RLSIGNAL,TSISIGNAL,HALFLIFE,CURRENTSTATE,ZSCORE,DTSMA200,ONEPCTRETURN,QSCORE,SPREAD,,STRUCTURE,TIMESTAMP,SIGNALTYPE,TIMESLOT
                
                if ( signalParam.signalType.equalsIgnoreCase("exitentry") || 
                        signalParam.signalType.equalsIgnoreCase("exitandentry") ||
                        signalParam.signalType.equalsIgnoreCase("entryexit") ||
                        signalParam.signalType.equalsIgnoreCase("entryandexit") ) {                   
                    processEntryExitSignal(signalParam);
                } else if (signalParam.signalType.equalsIgnoreCase("exitonlymatchingtimeslot")) {
                    processExitSignalForMatchingPosition(signalParam);               
                } else if (signalParam.signalType.equalsIgnoreCase("exitalltimeslots")) {
                    processExitSignalForAllPositions(signalParam);
                } else if (signalParam.signalType.equalsIgnoreCase("updatestoplosslimitmatchingtimeslot")) {
                    processUpdateStoplossLimitForMatchingPosition(signalParam);
                } else if (signalParam.signalType.equalsIgnoreCase("updatestoplosslimitalltimeslots")) {
                    processUpdateStoplossLimitForAllPositions(signalParam);
                }
                //write code to update take profit for all as well as matching timeslots
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
