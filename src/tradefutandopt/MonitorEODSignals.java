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
        String expiry;
        
        public MyExistingPositionClass(int timeSlot) {
            this.symbolName = "NA";
            this.positionSymbolType = "FUT";
            this.optionRightType = "NA";
            this.timeSlot = timeSlot;
            this.positionSideAndSize = 0;
            this.monitoringSymbolType = "IND";
            this.expiry = "20160000";
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
            this.monitoringContractType = "STK";
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
        
        // Structure of signal :
        //timstamp.",", - Time in YYYYMMDDHHMMSS format when signal ws pushed to queue
        //elementName,",", - NAME
        //elementLotSize,",", - LOTSIZE
        //qmatrixpred[1],",", - RlSIGNAL - +1 - golong, -1 - goshort, 0 - donothing, 999 or 9999 indicates have not seen this state before or error occured
        //tail(t_eod$region,n=1),",", - TSISIGNAL
        //halflife,",", - HALFLIFE defaulted to 70
        //qmatrixpred[3],",", - STATE
        //sprintf("%.2f",zscore),",", - ZSCORE
        //sprintf("%.2f",dtsma200),",", - DTSMA200
        //sprintf("%.2f",spread/100),",", - ONEPCTRETURN
        //sprintf("%.4f",qmatrixpred[2]),",", - QMATRIXPREDICTIONVALUE
        //sprintf("%.2f",spread),",", - SPREAD
        //futurestructure,",", - STRUCTURE
        //format(Sys.time(),"%Y%m%d%H%M%S",tz="Asia/Kolkata"),",", - TIMESTAMP
        //signalType,",", - SIGNALTYPE defaulted to exitandentry
        //timeSlot) - TIMESLOT in gap of 15 minutes
        
        // Index Values for Object
        final int EODSIGNALTIMEWHENGENERATED_INDEX = 0;        
        final int EODSIGNALSYMBOLNAME_INDEX = 1;
        final int EODSIGNALLOTSIZE_INDEX = 2;
        final int EODSIGNALRLSIGNAL_INDEX = 3;
        final int EODSIGNALTSISIGNAL_INDEX = 4;
        final int EODSIGNALHALFLIFE_INDEX = 5;        
        final int EODSIGNALSTATE_INDEX = 6;                
        final int EODSIGNALZSCORE_INDEX = 7;
        final int EODSIGNALDTSMA200_INDEX = 8;        
        final int EODSIGNALONEPCTRETURN_INDEX = 9;
        final int EODSIGNALQMATRIXPREDVAL_INDEX = 10;        
        final int EODSIGNALSPREAD_INDEX = 11;
        final int EODSIGNALSTRUCTURE_INDEX = 12;
        final int EODSIGNALTIMESTAMP_INDEX = 13;
        final int EODSIGNALSIGNALTYPE_INDEX = 14;
        final int EODSIGNALTIMESLOT_INDEX = 15;
        
        final int EODSIGNALMAX_ELEMENTS = 16;
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
            if (this.RLSignal > 99) {
                this.tradeSide = 0;
            }
            this.onePctReturn = Double.parseDouble(eodSignalObjectStructure[EODSIGNALONEPCTRETURN_INDEX]);
            this.spread = Double.parseDouble(eodSignalObjectStructure[EODSIGNALSPREAD_INDEX]);
            this.elementStructure = eodSignalObjectStructure[EODSIGNALSTRUCTURE_INDEX];
            this.signalTimeStamp = eodSignalObjectStructure[EODSIGNALTIMESTAMP_INDEX];           
            this.signalType = eodSignalObjectStructure[EODSIGNALSIGNALTYPE_INDEX];
            this.timeSlot = Integer.parseInt(eodSignalObjectStructure[EODSIGNALTIMESLOT_INDEX]);            
        } 
    }
    
    MonitorEODSignals(String name, JedisPool redisConnectionPool, String redisConfigKey, MyUtils utils, MyExchangeClass exchangeObj, boolean debugIndicator) {

        threadName = name;
        debugFlag = debugIndicator;
        jedisPool = redisConnectionPool;
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
        System.out.println(String.format("%1$tY%1$tm%1$td:%1$tH:%1$tM:%1$tS ", Calendar.getInstance(myExchangeObj.getExchangeTimeZone())) + 
                "Info : Monitoring End of Day Entry/Exit Signals for Strategy " + strategyName + 
                " EOD queue Name " + eodEntryExitSignalsQueueKeyName);
    }

    int getNearestNTHStrikePrice(double indexLevel, int NTH) {
        int returnValue = -1;
        
        returnValue = NTH * (int) (Math.round(indexLevel + (NTH / 2)) / NTH);
        
        return(returnValue);
    }
    
    int getNearestLowerNTHStrikePrice(double indexLevel, int NTH) {
        int returnValue = -1;
        
        returnValue = NTH * (int) (Math.round(indexLevel) / NTH);
        
        return(returnValue);
    }
    
    int getNearestHigherNTHStrikePrice(double indexLevel, int NTH) {
        int returnValue = -1;
        
        returnValue = NTH * (int) (Math.round(indexLevel + NTH) / NTH);
        
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

    int getNumPositionsGivenTimeSlotGivenSide(String symbolName, int timeSlot, int side) {
        // Find number of positions in current timeSlot.
        int numPositionsCurrentTimeSlot = 0;
        for ( Integer currentSlotNum : myExistingPositionMap.keySet()) {
            if ( (myExistingPositionMap.get(currentSlotNum).timeSlot == timeSlot) && 
                    myExistingPositionMap.get(currentSlotNum).symbolName.equalsIgnoreCase(symbolName) ) {
                if (myExistingPositionMap.get(currentSlotNum).positionSymbolType.equalsIgnoreCase("OPT") &&
                        (myExistingPositionMap.get(currentSlotNum).optionRightType.equalsIgnoreCase("CALL") || myExistingPositionMap.get(currentSlotNum).optionRightType.equalsIgnoreCase("C") ) &&
                        ( side > 0) ) {
                    numPositionsCurrentTimeSlot++;                    
                } else if (myExistingPositionMap.get(currentSlotNum).positionSymbolType.equalsIgnoreCase("OPT") &&
                        (myExistingPositionMap.get(currentSlotNum).optionRightType.equalsIgnoreCase("PUT") || myExistingPositionMap.get(currentSlotNum).optionRightType.equalsIgnoreCase("P") ) &&
                        ( side < 0) ) {
                    numPositionsCurrentTimeSlot++;                    
                } else if ( (myExistingPositionMap.get(currentSlotNum).positionSymbolType.equalsIgnoreCase("FUT") || myExistingPositionMap.get(currentSlotNum).positionSymbolType.equalsIgnoreCase("STK")) &&
                        ( myExistingPositionMap.get(currentSlotNum).positionSideAndSize > 0) &&
                        (side > 0) ) {
                    numPositionsCurrentTimeSlot++;                    
                } else if ( (myExistingPositionMap.get(currentSlotNum).positionSymbolType.equalsIgnoreCase("FUT") || myExistingPositionMap.get(currentSlotNum).positionSymbolType.equalsIgnoreCase("STK")) &&
                        ( myExistingPositionMap.get(currentSlotNum).positionSideAndSize < 0) &&
                        (side < 0) ) {
                    numPositionsCurrentTimeSlot++;                    
                }
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
                    tempExistingPositionObj.symbolName = myTradeObject.getTradingContractUnderlyingName();
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
                    tempExistingPositionObj.expiry = myTradeObject.getExpiry();
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
                        tempExistingPositionObject.symbolName = myTradeObject.getTradingContractUnderlyingName();
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
                        tempExistingPositionObject.expiry = myTradeObject.getExpiry();
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
                        tempExistingPositionObj.symbolName = myTradeObject.getTradingContractUnderlyingName();
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
                        tempExistingPositionObj.expiry = myTradeObject.getExpiry();
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
       
    void sendSquareOffSignal(String manualInterventionSignalsQueue, int slotNumber, String squareOffReason) {
        // perl code to push square off signal
        //$MISignalForRedis = $YYYYMMDDHHMMSS.",trade,".$positionID.",1,manualIntervention,0";
        //$redis->lpush($queueKeyName, $MISignalForRedis);
        
        Calendar timeNow = Calendar.getInstance(myExchangeObj.getExchangeTimeZone());        
        String squareOffSignal = String.format("%1$tY%1$tm%1$td%1$tH%1$tM%1$tS", timeNow) + ",trade," + slotNumber + ",1," + squareOffReason + ",0";

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
        if (signalParam.elementName.equalsIgnoreCase("NIFTY50") || signalParam.elementName.equalsIgnoreCase("BANKNIFTY")) {
            if (monitoringContractType.equalsIgnoreCase("STK")) {
                monitoringContractType = "IND";
            }
        } else {
            if (monitoringContractType.equalsIgnoreCase("IND")) {
                monitoringContractType = "STK";
            }
        }
        String monitorContractStructure = signalParam.elementName + "_" + monitoringContractType;        

        //subscription - structure contracttotrade:contracttomonitor:legsizemultiple:broker
        //e.g. 1015:OPT:IND:2:ZERODHA        
        String timeSlotSubscription = signalParam.timeSlot 
                + ":" + myTimeSlotSubscriptionsMap.get(signalParam.timeSlot).tradingContractType
                + ":" + monitoringContractType
                + ":" + myTimeSlotSubscriptionsMap.get(signalParam.timeSlot).legSizeMultiple
                + ":" + myTimeSlotSubscriptionsMap.get(signalParam.timeSlot).brokerToUse;

        int strikePriceIncrements = 100;
        if (signalParam.elementName.equalsIgnoreCase("NIFTY50") || signalParam.elementName.equalsIgnoreCase("BANKNIFTY")) {
            strikePriceIncrements = 100;
        } else if (signalParam.elementName.equalsIgnoreCase("SBIN")) {
            strikePriceIncrements = 5;
        } else if (signalParam.elementName.equalsIgnoreCase("TATASTEEL")) {
            strikePriceIncrements = 10;         
        }
        if (tradingContractType.equalsIgnoreCase("OPT")) {
            // this is for OPT type of contract
            String optionRightType = "CALL";
            int optionStrikePrice = getNearestNTHStrikePrice(signalParam.spread/signalParam.elementLotSize, strikePriceIncrements);            
            if (signalParam.tradeSide > 0) {
                optionRightType = "CALL";
                tradeSideAndSize = 1 * signalParam.tradeSide * legSizeMultiple;
                optionStrikePrice = getNearestLowerNTHStrikePrice(signalParam.spread/signalParam.elementLotSize, strikePriceIncrements);
            } else if (signalParam.tradeSide < 0) {
                optionRightType = "PUT";
                tradeSideAndSize = -1 * signalParam.tradeSide * legSizeMultiple;
                optionStrikePrice = getNearestHigherNTHStrikePrice(signalParam.spread/signalParam.elementLotSize, strikePriceIncrements);
            }
            optionStrikePrice = getNearestNTHStrikePrice(signalParam.spread/signalParam.elementLotSize, strikePriceIncrements);
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
    
    void squareOffAllLongPositions(String squareOffReason, String elementName) {
    
        // square off all long positions
        for ( Integer currentSlotNum : myExistingPositionMap.keySet()) {
            if (myExistingPositionMap.get(currentSlotNum).symbolName.equalsIgnoreCase(elementName) &&
                    (myExistingPositionMap.get(currentSlotNum).positionSymbolType.equalsIgnoreCase("FUT") ||
                    myExistingPositionMap.get(currentSlotNum).positionSymbolType.equalsIgnoreCase("STK")) ){
                //if LONG position exists for given symbol then square off if either of RLSIGNAL OR TSISIGNAL are -1
                if (myExistingPositionMap.get(currentSlotNum).positionSideAndSize > 0) {
                    sendSquareOffSignal(manualInterventionSignalsQueueKeyName,currentSlotNum,squareOffReason);
                }
            } else if (myExistingPositionMap.get(currentSlotNum).symbolName.equalsIgnoreCase(elementName) &&                        
                    myExistingPositionMap.get(currentSlotNum).positionSymbolType.equalsIgnoreCase("OPT") ){
                //if LONG position exists i.e. it is CALL OPTION then square off if either of RLSIGNAL OR TSISIGNAL are -1
                if (myExistingPositionMap.get(currentSlotNum).optionRightType.equalsIgnoreCase("CALL") || 
                        myExistingPositionMap.get(currentSlotNum).optionRightType.equalsIgnoreCase("C") ) {
                    sendSquareOffSignal(manualInterventionSignalsQueueKeyName,currentSlotNum,squareOffReason);
                }               
            }               
        }
    }
    
    void squareOffAllShortPositions(String squareOffReason, String elementName) {
    
        // square off all short positions
        for ( Integer currentSlotNum : myExistingPositionMap.keySet()) {
             if (myExistingPositionMap.get(currentSlotNum).symbolName.equalsIgnoreCase(elementName) &&
                     (myExistingPositionMap.get(currentSlotNum).positionSymbolType.equalsIgnoreCase("FUT") ||
                     myExistingPositionMap.get(currentSlotNum).positionSymbolType.equalsIgnoreCase("STK")) ){
                 //if SHORT position exists for given symbol then square off
                 if (myExistingPositionMap.get(currentSlotNum).positionSideAndSize < 0) {
                     sendSquareOffSignal(manualInterventionSignalsQueueKeyName,currentSlotNum,squareOffReason);
                 }
             } else if (myExistingPositionMap.get(currentSlotNum).symbolName.equalsIgnoreCase(elementName) &&                        
                     myExistingPositionMap.get(currentSlotNum).positionSymbolType.equalsIgnoreCase("OPT") ){
                 //if SHORT position exists i.e. it is PUT OPTION then square off
                 if (myExistingPositionMap.get(currentSlotNum).optionRightType.equalsIgnoreCase("PUT") || 
                         myExistingPositionMap.get(currentSlotNum).optionRightType.equalsIgnoreCase("P") ) {
                     sendSquareOffSignal(manualInterventionSignalsQueueKeyName,currentSlotNum,squareOffReason);
                 }
             }               
         }
    }
    
    void processRollOverOnly(MyEntrySignalParameters signalParam) {
        
        // Signal is zero OR position with same side as signal exists. check for roll over need and if roll over needed then rollover to next month expiry
        // check if there exists a position for current timeslot
        // && expiry of position is same as INRFUTPREVIOUSEXPIRY (for FUT) OR INROPTPREVIOUSEXPIRY (for OPT) 
        // then exit existing position and take new position with same side as original position with new expiry day.
        String previousFutExpiry = myUtils.getKeyValueFromRedis(jedisPool, "INRFUTPREVIOUSEXPIRY", debugFlag);
        String previousOptExpiry = myUtils.getKeyValueFromRedis(jedisPool, "INROPTPREVIOUSEXPIRY", debugFlag);

        for ( Integer currentSlotNum : myExistingPositionMap.keySet()) {
            if ( (myExistingPositionMap.get(currentSlotNum).timeSlot == signalParam.timeSlot) && 
                    myExistingPositionMap.get(currentSlotNum).symbolName.equalsIgnoreCase(signalParam.elementName) ) {
                if (myExistingPositionMap.get(currentSlotNum).positionSymbolType.equalsIgnoreCase("OPT") &&
                        (myExistingPositionMap.get(currentSlotNum).optionRightType.equalsIgnoreCase("CALL") || myExistingPositionMap.get(currentSlotNum).optionRightType.equalsIgnoreCase("C") ) &&
                        (myExistingPositionMap.get(currentSlotNum).expiry.matches(previousOptExpiry)) ) {
                    //rollover call option position
                    sendSquareOffSignal(manualInterventionSignalsQueueKeyName,currentSlotNum,"rolloverToNextExpiry");
                    myUtils.waitForNSeconds(30);                        
                    signalParam.RLSignal = 1;
                    signalParam.tradeSide = 1;
                    sendEntrySignal(entrySignalsQueueKeyName, signalParam);
                    signalParam.RLSignal = 0;
                    signalParam.tradeSide = 0;                        
                } else if (myExistingPositionMap.get(currentSlotNum).positionSymbolType.equalsIgnoreCase("OPT") &&
                        (myExistingPositionMap.get(currentSlotNum).optionRightType.equalsIgnoreCase("PUT") || myExistingPositionMap.get(currentSlotNum).optionRightType.equalsIgnoreCase("P") ) &&
                        (myExistingPositionMap.get(currentSlotNum).expiry.matches(previousOptExpiry)) ) {
                    // roll over put option position
                    sendSquareOffSignal(manualInterventionSignalsQueueKeyName,currentSlotNum,"rolloverToNextExpiry");
                    myUtils.waitForNSeconds(30);
                    signalParam.RLSignal = -1;
                    signalParam.tradeSide = -1;
                    sendEntrySignal(entrySignalsQueueKeyName, signalParam);
                    signalParam.RLSignal = 0;
                    signalParam.tradeSide = 0;                        
                } else if ( (myExistingPositionMap.get(currentSlotNum).positionSymbolType.equalsIgnoreCase("FUT") || myExistingPositionMap.get(currentSlotNum).positionSymbolType.equalsIgnoreCase("STK")) &&
                        ( myExistingPositionMap.get(currentSlotNum).positionSideAndSize > 0) &&
                        (myExistingPositionMap.get(currentSlotNum).expiry.matches(previousFutExpiry)) ) {
                    // roll over long future position
                    sendSquareOffSignal(manualInterventionSignalsQueueKeyName,currentSlotNum,"rolloverToNextExpiry");
                    myUtils.waitForNSeconds(30);                        
                    signalParam.RLSignal = 1;
                    signalParam.tradeSide = 1;
                    sendEntrySignal(entrySignalsQueueKeyName, signalParam);
                    signalParam.RLSignal = 0;
                    signalParam.tradeSide = 0;
                } else if ( (myExistingPositionMap.get(currentSlotNum).positionSymbolType.equalsIgnoreCase("FUT") || myExistingPositionMap.get(currentSlotNum).positionSymbolType.equalsIgnoreCase("STK")) &&
                        ( myExistingPositionMap.get(currentSlotNum).positionSideAndSize < 0) &&
                        (myExistingPositionMap.get(currentSlotNum).expiry.matches(previousFutExpiry)) ) {
                    // roll over short future position
                    sendSquareOffSignal(manualInterventionSignalsQueueKeyName,currentSlotNum,"rolloverToNextExpiry");
                    myUtils.waitForNSeconds(30);
                    signalParam.RLSignal = -1;
                    signalParam.tradeSide = -1;
                    sendEntrySignal(entrySignalsQueueKeyName, signalParam);
                    signalParam.RLSignal = 0;
                    signalParam.tradeSide = 0;                        
                }
            }
        }
    }

    void processEntryPartOfSignal(MyEntrySignalParameters signalParam) {
        // Now check if there exists any position of the same side as signal.
        // If no then take one position. 
        // If already exists a position of same side, then check for rollover day
        if ( (signalParam.RLSignal < 99) && (signalParam.tradeSide != 0) &&
                (getNumPositionsGivenTimeSlotGivenSide(signalParam.elementName, signalParam.timeSlot, signalParam.tradeSide) <= 0) ) {
                // since no position for given symbol for given side so take position as per RLSIGNAL
                sendEntrySignal(entrySignalsQueueKeyName, signalParam);
        }
    } // End of Method

    void processExitPartOfSignal(MyEntrySignalParameters signalParam) {

        if ((signalParam.RLSignal < 99) && (signalParam.RLSignal < 0 )) {
            // square off all long positions and take one short position
            squareOffAllLongPositions("exitSignal",signalParam.elementName);
        } else if ((signalParam.RLSignal < 99) && (signalParam.RLSignal > 0 )) {
            // square off all short positions and take one long position
            squareOffAllShortPositions("exitSignal",signalParam.elementName);
        }
    } // End of Method

    void processEntryAndExitSignal(MyEntrySignalParameters signalParam) {
        // RLSIGNAL - Reinforced Learning Signal - can take values 0, +1, -1 
        // 0 meaning non action necessary
        // +1 meaning recommended Buy if no position/Square off if existing Short position
        // -1 meaning recommended Sell if no position/Square off if existing Long position

        // trading rule implemented :
        // if no position for given symbol then take position as per RLSIGNAL
        // if RLSIGNAL is -1 then
        //   square off all long positions and take one short position
        // else if RLSIGNAL is +1 then
        //   square off all short positions and take one long position
        // for taking position, send form the signal and send to entrySignalsQueue
        // for square off, send signal to manual intervention queue

        if ((signalParam.RLSignal < 99) && (signalParam.RLSignal < 0 )) {
            // square off all long positions and take one short position
            squareOffAllLongPositions("exitSignal",signalParam.elementName);
        } else if ((signalParam.RLSignal < 99) && (signalParam.RLSignal > 0 )) {
            // square off all short positions and take one long position
            squareOffAllShortPositions("exitSignal",signalParam.elementName);
        }
        // Now check if there exists any position of the same side as signal.
        // If no then take one position. 
        // If already exists a position of same side, then check for rollover day
        if ( (signalParam.RLSignal < 99) && (signalParam.tradeSide != 0) &&
                (getNumPositionsGivenTimeSlotGivenSide(signalParam.elementName, signalParam.timeSlot, signalParam.tradeSide) <= 0) ) {
                // since no position for given symbol for given side so take position as per RLSIGNAL
                sendEntrySignal(entrySignalsQueueKeyName, signalParam);
        } else {
            processRollOverOnly(signalParam);
        }
    } // End of Method
        
    void processExitSignalForAllPositions(String exitReason, String elementName) {
        // trading rule implemented :
        // square off all positions
        squareOffAllLongPositions(exitReason, elementName);
        squareOffAllShortPositions(exitReason, elementName);
    } 
    
    boolean currentSignalSideMatchesCurrentNiftySignalSide(MyEntrySignalParameters signalParam) {    

        boolean retValue = false;
        
        Jedis jedis = jedisPool.getResource();
        
        // find index of signal of previous one hour slot
        int index = 0;
        String prevSignal = jedis.lindex("NIFTY50SIGNALSARCHIVE", index);
        int prevSignalTimeSlot = Integer.parseInt(prevSignal.split(",")[15]);
        int prevSignalRLSignal = Integer.parseInt(prevSignal.split(",")[3]);
        
        if (prevSignalTimeSlot != signalParam.timeSlot) {
            // proceed only if timeslots are same. if timeslot not matches, try next one as signals are half an hour interval
            index++;
            prevSignal = jedis.lindex("NIFTY50SIGNALSARCHIVE", index);
            prevSignalTimeSlot = Integer.parseInt(prevSignal.split(",")[15]);
            prevSignalRLSignal = Integer.parseInt(prevSignal.split(",")[3]);
        }

        System.out.println(String.format("%1$tY%1$tm%1$td:%1$tH:%1$tM:%1$tS ", Calendar.getInstance(myExchangeObj.getExchangeTimeZone())) + 
        "Info : Current Nifty Signal : " + prevSignal);
        jedisPool.returnResource(jedis);        
        
        if (prevSignalTimeSlot == signalParam.timeSlot) {
            // proceed only if timeslots are same.
            if (prevSignalRLSignal == signalParam.RLSignal) {
                retValue = true;
            } else {
                System.out.println(String.format("%1$tY%1$tm%1$td:%1$tH:%1$tM:%1$tS ", Calendar.getInstance(myExchangeObj.getExchangeTimeZone())) + 
                "Info : Current RL Signal does not match Current Nifty RL signal. Current Nifty Signal : " + prevSignal);            
            }            
        } else {
            System.out.println(String.format("%1$tY%1$tm%1$td:%1$tH:%1$tM:%1$tS ", Calendar.getInstance(myExchangeObj.getExchangeTimeZone())) + 
            "Info : Could not find Current Nifty RL signal having same timeslot as Current RL Signal. Current Nifty Signal : " + prevSignal);            
        }
        
        return(retValue);
    }

    boolean currentSignalSideOpposesCurrentNiftySignalSide(MyEntrySignalParameters signalParam) {    

        boolean retValue = false;
        
        Jedis jedis = jedisPool.getResource();
        
        // find index of signal of previous one hour slot
        int index = 0;
        String prevSignal = jedis.lindex("NIFTY50SIGNALSARCHIVE", index);
        int prevSignalTimeSlot = Integer.parseInt(prevSignal.split(",")[15]);
        int prevSignalRLSignal = Integer.parseInt(prevSignal.split(",")[3]);
        
        if (prevSignalTimeSlot != signalParam.timeSlot) {
            // proceed only if timeslots are same. if timeslot not matches, try next one as signals are half an hour interval
            index++;
            prevSignal = jedis.lindex("NIFTY50SIGNALSARCHIVE", index);
            prevSignalTimeSlot = Integer.parseInt(prevSignal.split(",")[15]);
            prevSignalRLSignal = Integer.parseInt(prevSignal.split(",")[3]);
        }

        System.out.println(String.format("%1$tY%1$tm%1$td:%1$tH:%1$tM:%1$tS ", Calendar.getInstance(myExchangeObj.getExchangeTimeZone())) + 
        "Info : Current Nifty Signal : " + prevSignal);
        jedisPool.returnResource(jedis);        
        
        if (prevSignalTimeSlot == signalParam.timeSlot) {
            // proceed only if timeslots are same.
            if (prevSignalRLSignal == (-1*signalParam.RLSignal) ) {
                retValue = true;
            } else {
                System.out.println(String.format("%1$tY%1$tm%1$td:%1$tH:%1$tM:%1$tS ", Calendar.getInstance(myExchangeObj.getExchangeTimeZone())) + 
                "Info : Current RL Signal does not oppose Current Nifty RL signal. Current Nifty Signal : " + prevSignal);            
            }            
        } else {
            System.out.println(String.format("%1$tY%1$tm%1$td:%1$tH:%1$tM:%1$tS ", Calendar.getInstance(myExchangeObj.getExchangeTimeZone())) + 
            "Info : Could not find Current Nifty RL signal having same timeslot as Current RL Signal. Current Nifty Signal : " + prevSignal);
        }
        
        return(retValue);
    }

    boolean currentSignalSideMatchesPrevNiftySignalSide(MyEntrySignalParameters signalParam) {    

        boolean retValue = false;
        
        Jedis jedis = jedisPool.getResource();
        
        // find index of signal of previous one hour slot
        int index = 0;
        String prevSignal = jedis.lindex("NIFTY50SIGNALSARCHIVE", index);
        int prevSignalTimeSlot = Integer.parseInt(prevSignal.split(",")[15]);
        int prevSignalRLSignal = Integer.parseInt(prevSignal.split(",")[3]);
        
        if (prevSignalTimeSlot == signalParam.timeSlot) {
            // move on as timeslots are same. most probably looking at same signal
            index++;
        }
        index = index + 1; // signals are at half hour interval.
        prevSignal = jedis.lindex("NIFTY50SIGNALSARCHIVE", index);
        prevSignalTimeSlot = Integer.parseInt(prevSignal.split(",")[15]);
        prevSignalRLSignal = Integer.parseInt(prevSignal.split(",")[3]);
        
        System.out.println(String.format("%1$tY%1$tm%1$td:%1$tH:%1$tM:%1$tS ", Calendar.getInstance(myExchangeObj.getExchangeTimeZone())) + 
        "Info : Prev Nifty Signal : " + prevSignal);
        jedisPool.returnResource(jedis);        

        if (prevSignalRLSignal == signalParam.RLSignal) {
            retValue = true;
        } else {
            System.out.println(String.format("%1$tY%1$tm%1$td:%1$tH:%1$tM:%1$tS ", Calendar.getInstance(myExchangeObj.getExchangeTimeZone())) + 
            "Info : Current RL Signal does not match Prev Nifty RL signal. prev Nifty Signal : " + prevSignal);            
        }
        return(retValue);
    }
    
    boolean currentSignalSideOpposesPrevNiftySignalSide(MyEntrySignalParameters signalParam) {    

        boolean retValue = false;
        
        Jedis jedis = jedisPool.getResource();
        
        // find index of signal of previous one hour slot
        int index = 0;
        String prevSignal = jedis.lindex("NIFTY50SIGNALSARCHIVE", index);
        int prevSignalTimeSlot = Integer.parseInt(prevSignal.split(",")[15]);
        int prevSignalRLSignal = Integer.parseInt(prevSignal.split(",")[3]);
        
        if (prevSignalTimeSlot == signalParam.timeSlot) {
            // move on as timeslots are same. most probably looking at same signal
            index++;
        }
        index = index + 1; // signals are at half hour interval.
        prevSignal = jedis.lindex("NIFTY50SIGNALSARCHIVE", index);
        prevSignalTimeSlot = Integer.parseInt(prevSignal.split(",")[15]);
        prevSignalRLSignal = Integer.parseInt(prevSignal.split(",")[3]);
        
        System.out.println(String.format("%1$tY%1$tm%1$td:%1$tH:%1$tM:%1$tS ", Calendar.getInstance(myExchangeObj.getExchangeTimeZone())) + 
        "Info : Prev Nifty Signal : " + prevSignal);
        jedisPool.returnResource(jedis);        

        if (prevSignalRLSignal == (-1*signalParam.RLSignal)) {
            retValue = true;
        } else {
            System.out.println(String.format("%1$tY%1$tm%1$td:%1$tH:%1$tM:%1$tS ", Calendar.getInstance(myExchangeObj.getExchangeTimeZone())) + 
            "Info : Current RL Signal does not oppose Prev Nifty RL signal. prev Nifty Signal : " + prevSignal);            
        }
        return(retValue);
    }
    
    boolean currentSignalSideMatchesPrevBankNiftySignalSide(MyEntrySignalParameters signalParam) {    

        boolean retValue = false;
        
        Jedis jedis = jedisPool.getResource();
        
        // find index of signal of previous one hour slot
        int index = 0;
        String prevSignal = jedis.lindex("BANKNIFTYSIGNALSARCHIVE", index);
        int prevSignalTimeSlot = Integer.parseInt(prevSignal.split(",")[15]);
        int prevSignalRLSignal = Integer.parseInt(prevSignal.split(",")[3]);
        
        if (prevSignalTimeSlot == signalParam.timeSlot) {
            // move on as timeslots are same. most probably looking at same signal
            index++;
        }
        index = index + 1; // signals are at half hour interval.
        prevSignal = jedis.lindex("BANKNIFTYSIGNALSARCHIVE", index);
        prevSignalTimeSlot = Integer.parseInt(prevSignal.split(",")[15]);
        prevSignalRLSignal = Integer.parseInt(prevSignal.split(",")[3]);
        
        System.out.println(String.format("%1$tY%1$tm%1$td:%1$tH:%1$tM:%1$tS ", Calendar.getInstance(myExchangeObj.getExchangeTimeZone())) + 
        "Info : Prev Bank Nifty Signal : " + prevSignal);
        jedisPool.returnResource(jedis);        

        if (prevSignalRLSignal == signalParam.RLSignal) {
            retValue = true;
        } else {
            System.out.println(String.format("%1$tY%1$tm%1$td:%1$tH:%1$tM:%1$tS ", Calendar.getInstance(myExchangeObj.getExchangeTimeZone())) + 
            "Info : Current RL Signal does not match Prev Bank Nifty RL signal. prev Bank Nifty Signal : " + prevSignal);            
        }
        return(retValue);
    }

    void performExitsForSymbols(String[] symbolsList) {
        
        int index = 0;
        if (symbolsList.length <= 0) {
            return;
        }
        Jedis jedis = jedisPool.getResource();
        
        for (index = 0; index < symbolsList.length; index++) {
            String symbolName = symbolsList[index];
            if (symbolName.equalsIgnoreCase("BANKNIFTY_WITH_NIFTY50")) {
                String queueName = "BANKNIFTYSIGNALSARCHIVE";
                String latestSignal = jedis.lindex(queueName, 0);
                MyEntrySignalParameters signalParam = new MyEntrySignalParameters(latestSignal);
                if ( (signalParam.RLSignal < 99) &&
                        currentSignalSideMatchesPrevBankNiftySignalSide(signalParam) &&
                        (!currentSignalSideOpposesCurrentNiftySignalSide(signalParam)) &&
                        (!currentSignalSideOpposesPrevNiftySignalSide(signalParam))
                      ) {
                    processExitPartOfSignal(signalParam);
                } 
                // Exit if current Nifty Signal and PrevNifty signal are opposite to current Banknifty positions
                // Find number of positions with long
                signalParam.RLSignal = 1;
                if (currentSignalSideMatchesCurrentNiftySignalSide(signalParam) &&
                        currentSignalSideMatchesPrevNiftySignalSide(signalParam)) {
                    // Nifty Signals are long. Exit all short positions OR PUT options
                    squareOffAllShortPositions("exitSignal",signalParam.elementName);
                }
                signalParam.RLSignal = -1;
                if (currentSignalSideMatchesCurrentNiftySignalSide(signalParam) &&
                        currentSignalSideMatchesPrevNiftySignalSide(signalParam)) {
                    // Nifty Signals are short. Exit all long positions OR CALL options
                    squareOffAllLongPositions("exitSignal",signalParam.elementName);                        
                }                
            } else {
                String queueName = symbolName + "SIGNALSARCHIVE";
                String latestSignal = jedis.lindex(queueName, 0);
                MyEntrySignalParameters signalParam = new MyEntrySignalParameters(latestSignal);
                if (signalParam.elementName.equalsIgnoreCase("NIFTY50")) {
                    if ( (signalParam.RLSignal < 99) && currentSignalSideMatchesPrevNiftySignalSide(signalParam) ) {
                        processExitPartOfSignal(signalParam);      
                    }
                } else {
                    processExitPartOfSignal(signalParam);
                }                
            }
        }
        
        jedisPool.returnResource(jedis);        
        return;
    }

    void performEntriesForSymbols(String[] symbolsList) {
        
        int index = 0;
        if (symbolsList.length <= 0) {
            return;
        }
        Jedis jedis = jedisPool.getResource();
        
        for (index = 0; index < symbolsList.length; index++) {
            String symbolName = symbolsList[index];
            if (symbolName.equalsIgnoreCase("BANKNIFTY_WITH_NIFTY50")) {
                String queueName = "BANKNIFTYSIGNALSARCHIVE";
                String latestSignal = jedis.lindex(queueName, 0);
                MyEntrySignalParameters signalParam = new MyEntrySignalParameters(latestSignal);
                if ( (signalParam.RLSignal < 99) &&
                        currentSignalSideMatchesPrevBankNiftySignalSide(signalParam) &&
                        (!currentSignalSideOpposesCurrentNiftySignalSide(signalParam)) &&
                        (!currentSignalSideOpposesPrevNiftySignalSide(signalParam))
                      ) {
                    processEntryPartOfSignal(signalParam);
                }
            } else {
                String queueName = symbolName + "SIGNALSARCHIVE";
                String latestSignal = jedis.lindex(queueName, 0);
                MyEntrySignalParameters signalParam = new MyEntrySignalParameters(latestSignal);
                if (signalParam.elementName.equalsIgnoreCase("NIFTY50")) {
                    if ( (signalParam.RLSignal < 99) && currentSignalSideMatchesPrevNiftySignalSide(signalParam) ) {
                        processEntryPartOfSignal(signalParam);      
                    }
                } else {
                    processEntryPartOfSignal(signalParam);
                }                
            }
        }
        
        jedisPool.returnResource(jedis);        
        return;
    }

    void performRolloverForSymbols(String[] symbolsList) {
        
        int index = 0;
        if (symbolsList.length <= 0) {
            return;
        }
        Jedis jedis = jedisPool.getResource();
        
        for (index = 0; index < symbolsList.length; index++) {
            String symbolName = symbolsList[index];
            if (symbolName.equalsIgnoreCase("BANKNIFTY_WITH_NIFTY50")) {
                String queueName = "BANKNIFTYSIGNALSARCHIVE";
                String latestSignal = jedis.lindex(queueName, 0);
                MyEntrySignalParameters signalParam = new MyEntrySignalParameters(latestSignal);
                processRollOverOnly(signalParam);
            } else {
                String queueName = symbolName + "SIGNALSARCHIVE";
                String latestSignal = jedis.lindex(queueName, 0);
                MyEntrySignalParameters signalParam = new MyEntrySignalParameters(latestSignal);
                processRollOverOnly(signalParam);
            }
        }
        
        jedisPool.returnResource(jedis);        
        return;
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
        // this queue just indicates the completion of siganls availability in the respective queues
        // while market is open keep monitoring eod signals queue
        while (myUtils.marketIsOpen(eodExitTime, myExchangeObj.getExchangeTimeZone(), false)) {
            eodSignalReceived = myUtils.popKeyValueFromQueueRedis(jedisPool, eodEntryExitSignalsQueueKeyName, 60, false);

            if (eodSignalReceived != null) {
                System.out.println(String.format("%1$tY%1$tm%1$td:%1$tH:%1$tM:%1$tS ", Calendar.getInstance(myExchangeObj.getExchangeTimeZone())) + 
                        "Info : Received Confirmation of availability of EOD Signals in corresponding Queues with message as : " + eodSignalReceived);

                /* Processing start */
                // update list symbols for which processing has to take place
                String[] symbolsList = myUtils.getHashMapValueFromRedis(jedisPool, redisConfigurationKey, "TRADESYMBOLS", false).split(",");

                // for each sysmbol in list of symbol - perform exits as applicable                
                // update current timeslot subscriptions and positions before performing exits
                updateCurrentTimeSlotSubscriptions();
                getExistingPositionDetailsForAllPositions(openPositionsQueueKeyName);
                performExitsForSymbols(symbolsList);

                // for each symbol in list of sysmbol - perform entries as applicable                
                // update current timeslot subscriptions and positions before performing enteries
                updateCurrentTimeSlotSubscriptions();
                getExistingPositionDetailsForAllPositions(openPositionsQueueKeyName);                
                performEntriesForSymbols(symbolsList);

                // for each symbol in list of symbols - perform rollover as applicable                
                // update current timeslot subscriptions and positions before performing rollovers
                updateCurrentTimeSlotSubscriptions();
                myUtils.waitForNSeconds(20); // Wait for 20 seconds before updating open positions and rollover
                getExistingPositionDetailsForAllPositions(openPositionsQueueKeyName);   
                performRolloverForSymbols(symbolsList);
                
                /* processing over */
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
