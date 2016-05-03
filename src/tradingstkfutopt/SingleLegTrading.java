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

/**
 * @author Manish Kumar Singh
 */
import redis.clients.jedis.*;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

public class SingleLegTrading {

    private static boolean debugFlag = false;

    private static JedisPool jedisPool;
    private static MyUtils myUtils;
    private static String redisConfigurationKey = "INRSTR01CONFIGS";

    private IBInteraction ibInteractionClient;

    private static String ibOrderIDKeyName;
    private static String exchangeHolidayListKeyName;

    public ConcurrentHashMap<String, MyManualInterventionClass> myMIDetails;

    private static MyExchangeClass myExchangeObj;

    SingleLegTrading(String redisIP, int redisPort, String redisConfigKey, boolean debugIndicator) {
        // Set Debug Flag 
        debugFlag = debugIndicator;
        // Create connection Pool for  Redis server. 
        JedisPoolConfig poolConfig = new JedisPoolConfig();
        poolConfig.setMaxTotal(128);
        poolConfig.setBlockWhenExhausted(false);
        jedisPool = new JedisPool(poolConfig, redisIP, redisPort);

        myUtils = new MyUtils();
        redisConfigurationKey = redisConfigKey;

        String ibIP = myUtils.getHashMapValueFromRedis(jedisPool, redisConfigurationKey, "IBTWSSERVERIPADDRESS", false);
        int ibPort = Integer.parseInt(myUtils.getHashMapValueFromRedis(jedisPool, redisConfigurationKey, "IBTWSSERVERPORTNUMBER", false));
        int ibClientID = Integer.parseInt(myUtils.getHashMapValueFromRedis(jedisPool, redisConfigurationKey, "IBTWSSERVERCLIENTID", false));
        String exchangeCurrency = myUtils.getHashMapValueFromRedis(jedisPool, redisConfigurationKey, "EXCHANGECURRENCY", false);

        myExchangeObj = new MyExchangeClass(exchangeCurrency);
        TimeZone.setDefault(myExchangeObj.getExchangeTimeZone());

        ibOrderIDKeyName = myUtils.getHashMapValueFromRedis(jedisPool, redisConfigurationKey, "ORDERIDFIELDKEYNAME", false);
        exchangeHolidayListKeyName = myUtils.getHashMapValueFromRedis(jedisPool, redisConfigurationKey, "EXCHANGEHOLIDAYLISTKEYNAME", false);
        ibInteractionClient = new IBInteraction(jedisPool, ibOrderIDKeyName, ibIP, ibPort, ibClientID, myUtils, myExchangeObj);

        myMIDetails = new ConcurrentHashMap<String, MyManualInterventionClass>();
    }

    /**
     * @param args the command line arguments
     */
    public static void main(String[] args) {
        // arg[0] is debug flag. 1 is true. 0 is false
        // arg[1] is ip address/machine name where Redis Server is running
        // arg[2] is port number on which Redis Server is listening for connections        
        // arg[3] is configKey in Redis Server to use

        // Read the Command Line Arguments
        if (args.length <= 3) {
            System.err.println("Number of Arguments can not be Less than 4");
            System.out.println(" Usage : <fileName.jar> <debugFlag : 0 or 1> <redisServerIPAddress> <redisServerPortNumber> <redisServerConfigKey>");
            System.exit(1);
        }

        boolean tempFlag = false;
        try {
            tempFlag = (Integer.parseInt(args[0]) == 1);
        } catch (NumberFormatException e) {
            System.err.println("First Argument must be an integer indicating Debug flag. Exiting..");
            System.exit(1);
        }

        SingleLegTrading myComboTradingSystem = new SingleLegTrading(args[1], Integer.parseInt(args[2]), args[3], tempFlag);

        // Set default timezone
        TimeZone.setDefault(myExchangeObj.getExchangeTimeZone());
        // Consolidate all open positions to serial from first position        
        myUtils.defragmentOpenPositionsQueue(jedisPool, redisConfigurationKey, true);
                
        if ((!myUtils.fallsOnExchangeHoliday("Exchange is closed today", myUtils.getKeyValueFromRedis(jedisPool, exchangeHolidayListKeyName, false), Calendar.getInstance(myExchangeObj.getExchangeTimeZone()), debugFlag))) {
            // Check if current time is outside Exchange Operating hours, then keep waiting for exchange to open

            myUtils.waitForStartTime(myExchangeObj.getExchangeStartTimeHHMM(), myExchangeObj.getExchangeTimeZone(), "Exchange to open", debugFlag);

            // Keep trying to establish Connection with IB Client till 15 minutes to market closing time 
            while ((!myComboTradingSystem.ibInteractionClient.connectToIB(120))
                    && (Integer.parseInt(String.format("%1$tH%1$tM", Calendar.getInstance(myExchangeObj.getExchangeTimeZone()))) < (myExchangeObj.getExchangeCloseTimeHHMM() - 12))) {
                myUtils.waitForNSeconds(300);
            }

            System.out.println(String.format("%1$tY%1$tm%1$td:%1$tH:%1$tM:%1$tS ", Calendar.getInstance(myExchangeObj.getExchangeTimeZone())) + " OrderID field " + ibOrderIDKeyName + " has been set for next Order Id as : " + myUtils.getKeyValueFromRedis(jedisPool, ibOrderIDKeyName, false));

            // Subscribe to Market Index
            int marketIndexReqId = 0;
            if (myExchangeObj.getExchangeCurrency().equalsIgnoreCase("INR")) {
                marketIndexReqId = myComboTradingSystem.ibInteractionClient.requestIndMktDataSubscription("NIFTY50");
            } else if (myExchangeObj.getExchangeCurrency().equalsIgnoreCase("USD")) {
                marketIndexReqId = myComboTradingSystem.ibInteractionClient.requestIndMktDataSubscription("SNP500");
            } 
            
            //Get order details of executed order from IB to redis hashmap - will help construct reports anytime irrespective of IB being available or not  
            myUtils.getOrderDetails2LocalDB(jedisPool, redisConfigurationKey, myComboTradingSystem.ibInteractionClient, true);
            
            // Spawn a thread to monitor the manual intervention signals queue
            MonitorManualInterventionSignals monitorManualInterventionSignalsQueue = new MonitorManualInterventionSignals("MonitoringManualInterventionsSignalsThread", jedisPool, redisConfigurationKey, myUtils, myExchangeObj, myComboTradingSystem.myMIDetails, debugFlag);
            monitorManualInterventionSignalsQueue.start();

            // Spawn a thread to monitor the entry signal queue
            MonitorEntrySignals monitorEntrySignalsQueue = new MonitorEntrySignals("MonitoringEntrySignalsThread", jedisPool, redisConfigurationKey, myUtils, myExchangeObj, myComboTradingSystem.ibInteractionClient,  debugFlag);
            monitorEntrySignalsQueue.start();

            // Spawn a thread to monitor EOD entry/exit signals queue
            MonitorEODSignals monitorEODEntryExitSignalsQueue = new MonitorEODSignals("MonitoringEODEntryExitSignalsThread", jedisPool, redisConfigurationKey, myUtils, myExchangeObj, myComboTradingSystem.ibInteractionClient,  debugFlag);
            monitorEODEntryExitSignalsQueue.start();
            
            // Spawn a thread to read the current open positions from Redis queue
            // For each open position, spawn another monitoring thread is spawned.
            MonitorOpenPositions4Exit monitorOpenPositionsQueue = new MonitorOpenPositions4Exit("MonitoringOpenPositionsForExitThread", jedisPool, redisConfigurationKey, myUtils, myExchangeObj, myComboTradingSystem.ibInteractionClient, myComboTradingSystem.myMIDetails, debugFlag);
            monitorOpenPositionsQueue.start();

            // Spawn a thread to read the current open positions from Redis queue
            // For each open position, if order status is "not completed Status", then try to update it using execDetails.
            MonitorOpenPositions4OrderCompletionStatus monitorOpenPositions4OrderStatus = new MonitorOpenPositions4OrderCompletionStatus("MonitoringOpenPositionsForOrderCompletionThread", jedisPool, redisConfigurationKey, myUtils, myExchangeObj, myComboTradingSystem.ibInteractionClient, debugFlag);
            monitorOpenPositions4OrderStatus.start();
            
            // Keep running the program till it is Exchange Closing time
            boolean exitNow = false;
            while (!exitNow) {
                Calendar timeNow = Calendar.getInstance(myExchangeObj.getExchangeTimeZone());
                // Provision for exiting if time has reached outside market hours or on weekends for NSE
                if (Integer.parseInt(String.format("%1$tH%1$tM%1$tS", timeNow)) >= myExchangeObj.getExchangeCloseTimeHHMMSS()) {
                    exitNow = true;
                    System.out.println(String.format("%1$tY%1$tm%1$td%1$tH%1$tM%1$tS", timeNow) + " : Exiting as reaching Outside market hours OR prescribed trading hours.");
                }
                // Wait for two minutes before next round of checking for exchange closure                
                if (!exitNow) {
                    myUtils.waitForNSeconds(120);                
                }
            }
            myComboTradingSystem.ibInteractionClient.cancelMktDataSubscription(marketIndexReqId);
            
            //Get order details of executed order from IB to redis hashmap - will help construct reports anytime irrespective of IB being available or not
            myUtils.getOrderDetails2LocalDB(jedisPool, redisConfigurationKey, myComboTradingSystem.ibInteractionClient, true);
            // Disconnect IB 
            myComboTradingSystem.ibInteractionClient.disconnectFromIB();
        }

        // Consolidate all open positions to serial from first position
        myUtils.defragmentOpenPositionsQueue(jedisPool, redisConfigurationKey, true);

        // Consolidate all current closed positions to Archived queue (PAST CLOSED POSITIONS)
        myUtils.moveCurrentClosedPositions2ArchiveQueue(jedisPool, redisConfigurationKey, false);

        // Release the connection Pool of Redis
        jedisPool.destroy();
        
        // Exit with Success 
        System.exit(0);
    }
}
