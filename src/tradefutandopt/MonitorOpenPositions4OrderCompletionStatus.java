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
import java.util.Map;
import java.util.TimeZone;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;

/**
 * @author Manish Kumar Singh
 */
public class MonitorOpenPositions4OrderCompletionStatus extends Thread {

    private Thread t;
    private String threadName;
    private boolean debugFlag;
    private JedisPool jedisPool;

    private IBInteraction ibInteractionClient;

    private MyExchangeClass myExchangeObj;
    private MyUtils myUtils;

    private String redisConfigurationKey;

    private String openPositionsQueueKeyName = "INRSTR01OPENPOSITIONS";
    private String closedPositionsQueueKeyName = "INRSTR01CLOSEDPOSITIONS";

    MonitorOpenPositions4OrderCompletionStatus(String name, JedisPool redisConnectionPool, String redisConfigKey, MyUtils utils, MyExchangeClass exchangeObj, IBInteraction ibIntClient, boolean debugIndicator) {

        threadName = name;
        debugFlag = debugIndicator;
        // Establish connection Pool Redis Server. 
        jedisPool = redisConnectionPool;
        ibInteractionClient = ibIntClient;
        redisConfigurationKey = redisConfigKey;
        myExchangeObj = exchangeObj;
        myUtils = utils;
        TimeZone.setDefault(myExchangeObj.getExchangeTimeZone());
        openPositionsQueueKeyName = myUtils.getHashMapValueFromRedis(jedisPool, redisConfigurationKey, "OPENPOSITIONSQUEUE", false);
        closedPositionsQueueKeyName = myUtils.getHashMapValueFromRedis(jedisPool, redisConfigurationKey, "CLOSEDPOSITIONSQUEUE", false);

    }

    @Override
    public void run() {

        myUtils.waitForStartTime(myExchangeObj.getExchangeStartTimeHHMM(), myExchangeObj.getExchangeTimeZone(), "Exit first order time", debugFlag);

        // Market is open. Now start monitoring the open positions queue
        int eodExitTime = myExchangeObj.getExchangeCloseTimeHHMM();;
        String eodExitTimeConfigValue = myUtils.getHashMapValueFromRedis(jedisPool, redisConfigurationKey, "EODEXITTIME", false);
        if ((eodExitTimeConfigValue != null) && (eodExitTimeConfigValue.length() > 0)) {
            eodExitTime = Integer.parseInt(eodExitTimeConfigValue);
        }

        while (myUtils.marketIsOpen(eodExitTime, myExchangeObj.getExchangeTimeZone(), false)) {

            Jedis jedis = jedisPool.getResource();
            Map<String, String> openPositionsMap = jedis.hgetAll(openPositionsQueueKeyName);
            jedisPool.returnResource(jedis);

            for (String keyMap : openPositionsMap.keySet()) {
                int slotNumber = Integer.parseInt(keyMap);

                TradingObject myTradeObject = new TradingObject(myUtils.getHashMapValueFromRedis(jedisPool, openPositionsQueueKeyName, Integer.toString(slotNumber), debugFlag));

                if (myTradeObject.getOrderState().equalsIgnoreCase("entryorderinitiated")) {
                    // Inititate the corrective steps if therre is gap of more than 15 minutes between order time and current time
                    String entryOrderTime = myTradeObject.getEntryTimeStamp();
                    String timeNow = String.format("%1$tY%1$tm%1$td%1$tH%1$tM%1$tS", Calendar.getInstance(myExchangeObj.getExchangeTimeZone()));
                    if (myUtils.checkIfStaleMessage(entryOrderTime, timeNow, 20)) {
                        // Get the order IDs
                        int entryOrderID = Integer.parseInt(myTradeObject.getTradingContractEntryOrderIDs());
                        // Get Execution details from IB
                        // update the openpositionqueue in redis to start monitoring
                    }
                } else if (myTradeObject.getOrderState().equalsIgnoreCase("exitordersenttoexchange")) {
                    // Inititate the corrective steps if therre is gap of more than 15 minutes between order time and current time                        
                    String exitOrderTime = myTradeObject.getTradingContractExitTimeStamp();
                    String timeNow = String.format("%1$tY%1$tm%1$td%1$tH%1$tM%1$tS", Calendar.getInstance(myExchangeObj.getExchangeTimeZone()));
                    if (exitOrderTime.length() > 8) {
                        if ((exitOrderTime.substring(0, 8).compareToIgnoreCase(timeNow.substring(0, 8)) < 0)
                                || ((exitOrderTime.substring(0, 8).compareToIgnoreCase(timeNow.substring(0, 8)) == 0) && (myUtils.checkIfStaleMessage(exitOrderTime, timeNow, 20)))) {
                            // Get the order IDs
                            int exitOrderID = Integer.parseInt(myTradeObject.getTradingContractExitOrderIDs());
                            if (exitOrderID > 0) {
                                // Get Execution details from IB
                                String symbolName = myTradeObject.getTradingContractUnderlyingName();
                                if (!(ibInteractionClient.myOrderStatusDetails.containsKey(exitOrderID))) {
                                    int requestId = ibInteractionClient.requestExecutionDetailsHistorical(5, symbolName);
                                    // wait till details are received OR for timeput to happen
                                    int timeOut = 0;
                                    while ((timeOut < 300)
                                            && (!(ibInteractionClient.requestsCompletionStatus.get(requestId)))                                  
                                            && (!(ibInteractionClient.myOrderStatusDetails.containsKey(exitOrderID)))) {
                                        myUtils.waitForNSeconds(20);
                                        timeOut = timeOut + 10;
                                    }
                                }
                                if (ibInteractionClient.myOrderStatusDetails.containsKey(exitOrderID)) {
                                    double exitSpread = ibInteractionClient.myOrderStatusDetails.get(exitOrderID).getFilledPrice()
                                            * ibInteractionClient.myOrderStatusDetails.get(exitOrderID).getFilledQuantity();
                                    Calendar cal = Calendar.getInstance(myExchangeObj.getExchangeTimeZone());
                                    cal.setTimeInMillis(ibInteractionClient.myOrderStatusDetails.get(exitOrderID).getUpdateTime());
                                    String exitTimeStamp = String.format("%1$tY%1$tm%1$td%1$tH%1$tM%1$tS", cal);
                                    // update the open position as well as closed position queue in Redis to update the details appropriately                            
                                    myUtils.updateAndMoveClosedPositions(jedisPool, openPositionsQueueKeyName, slotNumber, closedPositionsQueueKeyName, exitSpread, exitTimeStamp, exitOrderID, "exitorderfilled", "exitReasonUnknown", false);
                                }                                
                            }
                        }
                    }
                }
            } // end of checking of each slot                

            // Wait for Five minutes before checking again
            myUtils.waitForNSeconds(300);
        }
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
