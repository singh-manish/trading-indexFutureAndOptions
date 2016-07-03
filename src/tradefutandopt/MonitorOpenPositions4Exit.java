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

/**
 * @author Manish Kumar Singh
 */
public class MonitorOpenPositions4Exit extends Thread {

    private Thread t;
    private String threadName;
    private boolean debugFlag;
    private JedisPool jedisPool;

    private IBInteraction ibInteractionClient;

    private MyUtils myUtils;

    private String redisConfigurationKey;
    private MyExchangeClass myExchangeObj;
    private String openPositionsQueueKeyName = "INRSTR01OPENPOSITIONS";

    public ConcurrentHashMap<String, MyManualInterventionClass> myMIDetails;

    MonitorOpenPositions4Exit(String name, JedisPool redisConnectionPool, String redisConfigKey, MyUtils utils, MyExchangeClass exchangeObj, IBInteraction ibIntClient, ConcurrentHashMap<String, MyManualInterventionClass> miDetails, boolean debugIndicator) {

        threadName = name;
        debugFlag = debugIndicator;
        // Establish connection Pool Redis Server. 
        jedisPool = redisConnectionPool;
        ibInteractionClient = ibIntClient;
        redisConfigurationKey = redisConfigKey;
        myExchangeObj = exchangeObj;
        myUtils = utils;
        myMIDetails = miDetails;
        TimeZone.setDefault(myExchangeObj.getExchangeTimeZone());
    }

    @Override
    public void run() {

        // Initialize Values of queues and Max positions
        openPositionsQueueKeyName = myUtils.getHashMapValueFromRedis(jedisPool, redisConfigurationKey, "OPENPOSITIONSQUEUE", false);
        myUtils.waitForStartTime(myExchangeObj.getExchangeStartTimeHHMM(), myExchangeObj.getExchangeTimeZone(), "Exit first order time", debugFlag);

        // Market is open. Now start monitoring the open positions queue
        HashMap<Integer, Thread> exitThreadsMap = new HashMap<Integer, Thread>();
        HashMap<Integer, SingleLegExit> exitObjectsMap = new HashMap<Integer, SingleLegExit>();

        int eodExitTime = 1530;
        String eodExitTimeConfigValue = myUtils.getHashMapValueFromRedis(jedisPool, redisConfigurationKey, "EODEXITTIME", false);
        if ((eodExitTimeConfigValue != null) && (eodExitTimeConfigValue.length() > 0)) {
            eodExitTime = Integer.parseInt(eodExitTimeConfigValue);
        }

        while (myUtils.marketIsOpen(eodExitTime, myExchangeObj.getExchangeTimeZone(), false)) {

            int lastExitOrderTime = myExchangeObj.getExchangeCloseTimeHHMM();
            String lastExitOrderTimeConfigValue = myUtils.getHashMapValueFromRedis(jedisPool, redisConfigurationKey, "LASTEXITORDERTIME", false);
            if ((lastExitOrderTimeConfigValue != null) && (lastExitOrderTimeConfigValue.length() > 0)) {
                lastExitOrderTime = Integer.parseInt(lastExitOrderTimeConfigValue);
            }
            if (myUtils.marketIsOpen(lastExitOrderTime, myExchangeObj.getExchangeTimeZone(), false)) {
                Jedis jedis = jedisPool.getResource();
                Map<String, String> openPositionsMap = jedis.hgetAll(openPositionsQueueKeyName);
                jedisPool.returnResource(jedis);

                for (String keyMap : openPositionsMap.keySet()) {
                    int slotNumber = Integer.parseInt(keyMap);

                    TradingObject myTradeObject = new TradingObject(openPositionsMap.get(keyMap));
                    if (myTradeObject.getOrderState().equalsIgnoreCase("entryorderfilled")) {
                        // Check if corresponding thread is running. if not running then start a thread to monitor the position
                        String exitMonitoringThreadName = "monitoringExit4Position_" + Integer.toString(slotNumber);
                        // if this is first time then thread object would be null. if it is null then create the instance
                        if (!(exitThreadsMap.containsKey(slotNumber))) {
                            exitThreadsMap.put(slotNumber, null);
                        }
                        if (exitThreadsMap.get(slotNumber) == null) {
                            exitObjectsMap.put(slotNumber, new SingleLegExit(exitMonitoringThreadName, jedisPool, ibInteractionClient, redisConfigurationKey, myUtils, myExchangeObj.getExchangeTimeZone(), slotNumber, myMIDetails, debugFlag));
                            exitThreadsMap.put(slotNumber, new Thread(exitObjectsMap.get(slotNumber)));
                            exitThreadsMap.get(slotNumber).setName(exitMonitoringThreadName);
                            exitThreadsMap.get(slotNumber).start();
                        } else if (exitThreadsMap.get(slotNumber).getState() == Thread.State.TERMINATED) {
                            System.out.println(String.format("%1$tY%1$tm%1$td:%1$tH:%1$tM:%1$tS ", Calendar.getInstance(myExchangeObj.getExchangeTimeZone())) + "Info : Restarting Monitoring Thread for Leg " + myTradeObject.getTradingObjectName() + " having state as " + exitThreadsMap.get(slotNumber).getState() + " having Name as " + exitThreadsMap.get(slotNumber).getName() + " having alive status as " + exitThreadsMap.get(slotNumber).isAlive());
                            exitObjectsMap.put(slotNumber, new SingleLegExit(exitMonitoringThreadName, jedisPool, ibInteractionClient, redisConfigurationKey, myUtils, myExchangeObj.getExchangeTimeZone(), slotNumber, myMIDetails, debugFlag));
                            exitThreadsMap.put(slotNumber, new Thread(exitObjectsMap.get(slotNumber)));
                            exitThreadsMap.get(slotNumber).setName(exitMonitoringThreadName);
                            exitThreadsMap.get(slotNumber).start();
                        }
                    } else {
                        System.out.println(String.format("%1$tY%1$tm%1$td:%1$tH:%1$tM:%1$tS ", Calendar.getInstance(myExchangeObj.getExchangeTimeZone())) + "Info : Not Able to Start Monitoring for Leg " + myTradeObject.getTradingObjectName() + " as Entry Order is not updated as completely filled in open positions queue");
                    }
                } // end of For loop of checking against each position

            }
            // Wait for one minutes before checking again
            myUtils.waitForNSeconds(60);
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
