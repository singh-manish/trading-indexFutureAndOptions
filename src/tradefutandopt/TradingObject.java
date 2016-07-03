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

import java.text.DecimalFormat;

/**
 * @author Manish Kumar Singh
 */
public class TradingObject {

    // Index Values for Object
    public static final int ENTRY_TIMESTAMP_INDEX = 0;
    public static final int NAME_INDEX = 1;
    public static final int SIDE_SIZE_INDEX = 2;
    public static final int TRADING_CONTRACT_STRUCTURE_INDEX = 3;
    public static final int TRADING_CONTRACT_ENTRY_STDDEV_INDEX = 4;    
    public static final int MONITORING_CONTRACT_STRUCTURE_INDEX = 5;
    public static final int MONITORING_CONTRACT_ENTRY_STDDEV_INDEX = 6;
    public static final int TIMESLOT_SUBSCRIPTION_INDEX = 7;
    public static final int ORDER_STATE_INDEX = 8;   
    public static final int MONITORING_CONTRACT_ENTRY_PRICE_INDEX = 9;
    public static final int MONITORING_CONTRACT_LAST_KNOWN_PRICE_INDEX = 10;
    public static final int MONITORING_CONTRACT_LAST_UPDATED_TIMESTAMP_INDEX = 11;    
    public static final int MONITORING_CONTRACT_LOWER_BREACH_INDEX = 12;
    public static final int MONITORING_CONTRACT_UPPER_BREACH_INDEX = 13;
    public static final int TRADING_CONTRACT_EXPIRY_INDEX = 14;    
    public static final int TRADING_CONTRACT_ENTRY_SPREAD_INDEX = 15;    
    public static final int TRADING_CONTRACT_ENTRY_BID_ASK_FILL_INDEX = 16;
    public static final int TRADING_CONTRACT_ENTRY_ORDERIDS_INDEX = 17;
    public static final int TRADING_CONTRACT_LAST_KNOWN_SPREAD_INDEX = 18;
    public static final int TRADING_CONTRACT_LAST_UPDATED_TIMESTAMP_INDEX = 19;
    public static final int TRADING_CONTRACT_EXIT_REASON_INDEX = 20;        
    public static final int TRADING_CONTRACT_EXIT_SPREAD_INDEX = 21;
    public static final int TRADING_CONTRACT_EXIT_TIMESTAMP_INDEX = 22;
    public static final int TRADING_CONTRACT_EXIT_ORDERIDS_INDEX = 23;
    public static final int TRADING_CONTRACT_EXIT_BID_ASK_FILL_INDEX = 24;
   
    public static final int MAX_NUM_ELEMENTS = 25;

    private String[] tradeObjectStructure;

    // Index Values for contractStructure
    // contractStructure is <underlyingSymbol>_<lotSize>_<contractType>_<optionType>_<optionStrike>
    // e.g. SBIN_300_STK
    // e.g. NIFTY50_75_FUT
    // e.g. NIFTY50_75_OPT_PUT_7500.0 OR NIFTY50_75_OPT_CALL_8500.0    
    public static final int TRADING_CONTRACT_STRUCT_UNDERLYING_INDEX = 0;
    public static final int TRADING_CONTRACT_STRUCT_LOTSIZE_INDEX = 1;
    public static final int TRADING_CONTRACT_STRUCT_TYPE_INDEX = 2;
    public static final int TRADING_CONTRACT_STRUCT_OPTIONRIGHT_INDEX = 3;
    public static final int TRADING_CONTRACT_STRUCT_OPTIONSTRIKE_INDEX = 4;    

    // Index Values for MonioringContractStructure
    // contractStructure is <underlyingSymbol>_<contractType>
    // e.g. SBIN_STK
    // e.g. NIFTY50_FUT
    // e.g. NIFTY50_IND    
    public static final int MONITORING_CONTRACT_STRUCT_UNDERLYING_INDEX = 0;
    public static final int MONITORING_CONTRACT_STRUCT_TYPE_INDEX = 1;
    
    //subscription - structure contracttotrade:contracttomonitor:legsizemultiple:broker
    //e.g. 1015:OPT:IND:2:ZERODHA
    //means subscribed for 10:15 timeslot with options to trade but index to monitor and legsize is twice of lotsize with 
    //broker as ZERODHA
    //contract to trade could be OPT or FUT or STK. contract to monitor could be IND which is INDEX or STK or OPT or FUT. 
    //broker could be IB or ZERODHA
    public static final int TIMESLOT_SUBSCRIPTION_TIMESLOT_INDEX = 0;
    public static final int TIMESLOT_SUBSCRIPTION_TRADINGCONTRACTTYPE_INDEX = 1;
    public static final int TIMESLOT_SUBSCRIPTION_MONITORINGCONTRACTTYPE_INDEX = 2;
    public static final int TIMESLOT_SUBSCRIPTION_LEGSIZEMULTIPLE_INDEX = 3;
    public static final int TIMESLOT_SUBSCRIPTION_BROKERNAME_INDEX = 4;
        
    TradingObject(String incomingTradeObject) {

        tradeObjectStructure = new String[MAX_NUM_ELEMENTS];

        if (incomingTradeObject.length() > 0) {
            String[] tempTradeObjectStructure = incomingTradeObject.split(",");
            for (int index = 0; index < tempTradeObjectStructure.length; index++) {
                if (index < MAX_NUM_ELEMENTS) {
                    tradeObjectStructure[index] = tempTradeObjectStructure[index];                    
                }
            }
        }
    }

    // assemble and return Structure
    public String getCompleteTradingObjectString() {
        String returnString = "";

        for (int index = 0; index < (tradeObjectStructure.length - 1); index++) {
            returnString = returnString + tradeObjectStructure[index] + ",";
        }

        returnString = returnString + tradeObjectStructure[tradeObjectStructure.length - 1];

        return (returnString);
    }

    public void initiateAndValidate() {
        
        if ((tradeObjectStructure[TRADING_CONTRACT_ENTRY_STDDEV_INDEX] != null)
                && (tradeObjectStructure[TRADING_CONTRACT_ENTRY_STDDEV_INDEX].length() > 0)) {
            DecimalFormat myDf = new DecimalFormat("0.00");
            tradeObjectStructure[TRADING_CONTRACT_ENTRY_STDDEV_INDEX] = myDf.format(Double.valueOf(tradeObjectStructure[TRADING_CONTRACT_ENTRY_STDDEV_INDEX]));
        }

        if ((tradeObjectStructure[MONITORING_CONTRACT_ENTRY_STDDEV_INDEX] != null)
                && (tradeObjectStructure[MONITORING_CONTRACT_ENTRY_STDDEV_INDEX].length() > 0)) {
            DecimalFormat myDf = new DecimalFormat("0.00");
            tradeObjectStructure[MONITORING_CONTRACT_ENTRY_STDDEV_INDEX] = myDf.format(Double.valueOf(tradeObjectStructure[MONITORING_CONTRACT_ENTRY_STDDEV_INDEX]));
        }
        
        if ((tradeObjectStructure[ORDER_STATE_INDEX] == null)
                || (tradeObjectStructure[ORDER_STATE_INDEX].length() <= 0)) {
            tradeObjectStructure[ORDER_STATE_INDEX] = "signalreceived";
        }  
            
        if ((tradeObjectStructure[MONITORING_CONTRACT_ENTRY_PRICE_INDEX] == null)
                || (tradeObjectStructure[MONITORING_CONTRACT_ENTRY_PRICE_INDEX].length() <= 0)) {
            tradeObjectStructure[MONITORING_CONTRACT_ENTRY_PRICE_INDEX] = String.format("%.2f", 0.0);
        } else {
            DecimalFormat myDf = new DecimalFormat("0.00");
            tradeObjectStructure[MONITORING_CONTRACT_ENTRY_PRICE_INDEX] = myDf.format(Double.valueOf(tradeObjectStructure[MONITORING_CONTRACT_ENTRY_PRICE_INDEX]));
        }        
        
        if ((tradeObjectStructure[MONITORING_CONTRACT_LAST_KNOWN_PRICE_INDEX] == null)
                || (tradeObjectStructure[MONITORING_CONTRACT_LAST_KNOWN_PRICE_INDEX].length() <= 0)) {
            tradeObjectStructure[MONITORING_CONTRACT_LAST_KNOWN_PRICE_INDEX] = String.format("%.2f", 0.0);
        } else {
            DecimalFormat myDf = new DecimalFormat("0.00");
            tradeObjectStructure[MONITORING_CONTRACT_LAST_KNOWN_PRICE_INDEX] = myDf.format(Double.valueOf(tradeObjectStructure[MONITORING_CONTRACT_LAST_KNOWN_PRICE_INDEX]));
        }
        
        if ((tradeObjectStructure[MONITORING_CONTRACT_LAST_UPDATED_TIMESTAMP_INDEX] == null)
                || (tradeObjectStructure[MONITORING_CONTRACT_LAST_UPDATED_TIMESTAMP_INDEX].length() <= 0)) {
            tradeObjectStructure[MONITORING_CONTRACT_LAST_UPDATED_TIMESTAMP_INDEX] = String.format("%d", -1);
        }   
       
        if ((tradeObjectStructure[MONITORING_CONTRACT_LOWER_BREACH_INDEX] == null)
                || (tradeObjectStructure[MONITORING_CONTRACT_LOWER_BREACH_INDEX].length() <= 0)) {
            tradeObjectStructure[MONITORING_CONTRACT_LOWER_BREACH_INDEX] = String.format("%.2f", 0.0);
        } else {
            DecimalFormat myDf = new DecimalFormat("0.00");
            tradeObjectStructure[MONITORING_CONTRACT_LOWER_BREACH_INDEX] = myDf.format(Double.valueOf(tradeObjectStructure[MONITORING_CONTRACT_LOWER_BREACH_INDEX]));
        }

       
        if ((tradeObjectStructure[MONITORING_CONTRACT_LOWER_BREACH_INDEX] == null)
                || (tradeObjectStructure[MONITORING_CONTRACT_LOWER_BREACH_INDEX].length() <= 0)) {
            tradeObjectStructure[MONITORING_CONTRACT_LOWER_BREACH_INDEX] = String.format("%.2f", 0.0);
        } else {
            DecimalFormat myDf = new DecimalFormat("0.00");
            tradeObjectStructure[MONITORING_CONTRACT_LOWER_BREACH_INDEX] = myDf.format(Double.valueOf(tradeObjectStructure[MONITORING_CONTRACT_LOWER_BREACH_INDEX]));
        }
        
        if ((tradeObjectStructure[MONITORING_CONTRACT_UPPER_BREACH_INDEX] == null)
                || (tradeObjectStructure[MONITORING_CONTRACT_UPPER_BREACH_INDEX].length() <= 0)) {
            tradeObjectStructure[MONITORING_CONTRACT_UPPER_BREACH_INDEX] = String.format("%.2f", 0.0);
        } else {
            DecimalFormat myDf = new DecimalFormat("0.00");
            tradeObjectStructure[MONITORING_CONTRACT_UPPER_BREACH_INDEX] = myDf.format(Double.valueOf(tradeObjectStructure[MONITORING_CONTRACT_UPPER_BREACH_INDEX]));
        }        
                    
        if ((tradeObjectStructure[TRADING_CONTRACT_ENTRY_SPREAD_INDEX] == null)
                || (tradeObjectStructure[TRADING_CONTRACT_ENTRY_SPREAD_INDEX].length() <= 0)) {
            tradeObjectStructure[TRADING_CONTRACT_ENTRY_SPREAD_INDEX] = String.format("%.2f", 0.0);
        } else {
            DecimalFormat myDf = new DecimalFormat("0.00");
            tradeObjectStructure[TRADING_CONTRACT_ENTRY_SPREAD_INDEX] = myDf.format(Double.valueOf(tradeObjectStructure[TRADING_CONTRACT_ENTRY_SPREAD_INDEX]));
        }        
        
        if ((tradeObjectStructure[TRADING_CONTRACT_LAST_KNOWN_SPREAD_INDEX] == null)
                || (tradeObjectStructure[TRADING_CONTRACT_LAST_KNOWN_SPREAD_INDEX].length() <= 0)) {
            tradeObjectStructure[TRADING_CONTRACT_LAST_KNOWN_SPREAD_INDEX] = String.format("%.2f", 0.0);
        } else {
            DecimalFormat myDf = new DecimalFormat("0.00");
            tradeObjectStructure[TRADING_CONTRACT_LAST_KNOWN_SPREAD_INDEX] = myDf.format(Double.valueOf(tradeObjectStructure[TRADING_CONTRACT_LAST_KNOWN_SPREAD_INDEX]));
        }
        
        if ((tradeObjectStructure[TRADING_CONTRACT_LAST_UPDATED_TIMESTAMP_INDEX] == null)
                || (tradeObjectStructure[TRADING_CONTRACT_LAST_UPDATED_TIMESTAMP_INDEX].length() <= 0)) {
            tradeObjectStructure[TRADING_CONTRACT_LAST_UPDATED_TIMESTAMP_INDEX] = String.format("%d", -1);
        }

    }

    public void setEntryTimeStamp(String newTimeStamp) {
        tradeObjectStructure[ENTRY_TIMESTAMP_INDEX] = newTimeStamp;
    }

    public void setSideAndSize(int side, int size) {
        String sideAndSize = "1";
        sideAndSize = Integer.toString(size * side);
        tradeObjectStructure[SIDE_SIZE_INDEX] = sideAndSize;
    }

    public void setTradingContractEntryStdDev(double newEntryStdDev) {
        tradeObjectStructure[TRADING_CONTRACT_ENTRY_STDDEV_INDEX] = Double.toString(newEntryStdDev);
    }
    
    public void setMonitoringContractEntryStdDev(double newEntryStdDev) {
        tradeObjectStructure[MONITORING_CONTRACT_ENTRY_STDDEV_INDEX] = Double.toString(newEntryStdDev);
    }
    
    public void setOrderState(String newState) {
        tradeObjectStructure[ORDER_STATE_INDEX] = newState;
    }

    public void setMonitoringContractEntryPrice(double newPrice) {
        DecimalFormat myDf = new DecimalFormat("0.00");
        tradeObjectStructure[MONITORING_CONTRACT_ENTRY_PRICE_INDEX] = myDf.format(Double.valueOf(newPrice));
    }
    
    public void setMonitoringContractLastKnownPrice(double newSpread) {
        tradeObjectStructure[MONITORING_CONTRACT_LAST_KNOWN_PRICE_INDEX] = Double.toString(newSpread);
    }

    public void setMonitoringContractLastUpdatedTimeStamp(String newTimeStamp) {
        tradeObjectStructure[MONITORING_CONTRACT_LAST_UPDATED_TIMESTAMP_INDEX] = newTimeStamp;
    }

    public void setMonitoringContractLowerBreach(int newBreach) {
        tradeObjectStructure[MONITORING_CONTRACT_LOWER_BREACH_INDEX] = Integer.toString(newBreach);
    }

    public void setMonitoringContractLowerBreach(double newBreach) {
        DecimalFormat myDf = new DecimalFormat("0.00");
        tradeObjectStructure[MONITORING_CONTRACT_LOWER_BREACH_INDEX] = myDf.format(Double.valueOf(newBreach));
    }

    public void setMonitoringContractUpperBreach(int newBreach) {
        tradeObjectStructure[MONITORING_CONTRACT_UPPER_BREACH_INDEX] = Integer.toString(newBreach);
    }

    public void setMonitoringContractUpperBreach(double newBreach) {
        DecimalFormat myDf = new DecimalFormat("0.00");
        tradeObjectStructure[MONITORING_CONTRACT_UPPER_BREACH_INDEX] = myDf.format(Double.valueOf(newBreach));
    }

    public void setExpiry(int expiry) {
        tradeObjectStructure[TRADING_CONTRACT_EXPIRY_INDEX] = Integer.toString(expiry);
    }

    public void setExpiry(String expiry) {
        tradeObjectStructure[TRADING_CONTRACT_EXPIRY_INDEX] = expiry;
    }
    
    public void setTradingContractExpiry(int expiry) {
        tradeObjectStructure[TRADING_CONTRACT_EXPIRY_INDEX] = Integer.toString(expiry);
    }

    public void setTradingContractExpiry(String expiry) {
        tradeObjectStructure[TRADING_CONTRACT_EXPIRY_INDEX] = expiry;
    }
        
    public void setTradingContractEntrySpread(double newSpread) {
        DecimalFormat myDf = new DecimalFormat("0.00");
        tradeObjectStructure[TRADING_CONTRACT_ENTRY_SPREAD_INDEX] = myDf.format(Double.valueOf(newSpread));
    }
    
    public void setTradingContractEntryBidAskFillDetails(String newBidAskFillDetails) {
        tradeObjectStructure[TRADING_CONTRACT_ENTRY_BID_ASK_FILL_INDEX] = newBidAskFillDetails;
    }

    public void setTradingContractEntryOrderIDs(String newOrderIDs) {
        tradeObjectStructure[TRADING_CONTRACT_ENTRY_ORDERIDS_INDEX] = newOrderIDs;
    }
    
    public void setTradingContractEntryOrderIDs(int entryOrderID) {
        tradeObjectStructure[TRADING_CONTRACT_ENTRY_ORDERIDS_INDEX] = Integer.toString(entryOrderID);
    }
    
    public void setTradingContractLastKnownSpread(double newSpread) {
        tradeObjectStructure[TRADING_CONTRACT_LAST_KNOWN_SPREAD_INDEX] = Double.toString(newSpread);
    }

    public void setTradingContractLastUpdatedTimeStamp(String newTimeStamp) {
        tradeObjectStructure[TRADING_CONTRACT_LAST_UPDATED_TIMESTAMP_INDEX] = newTimeStamp;
    }
    
    public void setTradingContractExitReason(String exitReason) {
        tradeObjectStructure[TRADING_CONTRACT_EXIT_REASON_INDEX] = exitReason;
    }    

    public void setTradingContractExitSpread(double newSpread) {
        DecimalFormat myDf = new DecimalFormat("0.00");
        tradeObjectStructure[TRADING_CONTRACT_EXIT_SPREAD_INDEX] = myDf.format(Double.valueOf(newSpread));
    }

    public void setTradingContractExitTimeStamp(String newTimeStamp) {
        tradeObjectStructure[TRADING_CONTRACT_EXIT_TIMESTAMP_INDEX] = newTimeStamp;
    }

    public void setTradingContractExitOrderIDs(int exitOrderID) {
        tradeObjectStructure[TRADING_CONTRACT_EXIT_ORDERIDS_INDEX] = Integer.toString(exitOrderID);
    }

    public void setTradingContractExitOrderIDs(String newOrderIDs) {
        tradeObjectStructure[TRADING_CONTRACT_EXIT_ORDERIDS_INDEX] = newOrderIDs;
    }

    public void setTradingContractExitBidAskFillDetails(String newBidAskFillDetails) {
        tradeObjectStructure[TRADING_CONTRACT_EXIT_BID_ASK_FILL_INDEX] = newBidAskFillDetails;
    }

    public String getEntryTimeStamp() {
        return (tradeObjectStructure[ENTRY_TIMESTAMP_INDEX]);
    }

    public String getTradingObjectName() {
        return (tradeObjectStructure[NAME_INDEX]);
    }

    public int getSideAndSize() {
        
        int sideAndSize = 0; // Default value
     
        try {
            sideAndSize = Integer.parseInt(tradeObjectStructure[SIDE_SIZE_INDEX]);
        } catch (NumberFormatException | NullPointerException ex) {
            sideAndSize = 0;
        }            
        return (sideAndSize);
    }        

    
    public String getTradingContractStructure() {
        // contractStructure is <underlyingSymbol>_<lotSize>_<contractType>_<optionType>_<optionStrike>
        // e.g. SBIN_300_STK
        // e.g. NIFTY50_75_FUT
        // e.g. NIFTY50_75_OPT_PUT_7500.0 OR NIFTY50_75_OPT_CALL_8500.0
        //CONTRACT_STRUCT_UNDERLYING_INDEX = 0;
        //CONTRACT_STRUCT_LOTSIZE_INDEX = 1;
        //CONTRACT_STRUCT_TYPE_INDEX = 2;
        //CONTRACT_STRUCT_OPTIONRIGHT_INDEX = 3;
        //CONTRACT_STRUCT_OPTIONSTRIKE_INDEX = 4;           
        
        return (tradeObjectStructure[TRADING_CONTRACT_STRUCTURE_INDEX]);
    }   

    public String getTradingContractUnderlyingName() {
        
        String underlyingName = "UNDERLYING"; // Default value
        String[] contractStructure = this.getTradingContractStructure().split("_");
        if (contractStructure.length >= TRADING_CONTRACT_STRUCT_UNDERLYING_INDEX) {
            underlyingName = contractStructure[TRADING_CONTRACT_STRUCT_UNDERLYING_INDEX];
        }
        return (underlyingName);
    }    
    
    public int getTradingContractLotSize() {
        
        int lotSize = 0; // Default value
        String[] contractStructure = this.getTradingContractStructure().split("_");
        if (contractStructure.length >= TRADING_CONTRACT_STRUCT_LOTSIZE_INDEX) {          
            try {
                lotSize = Integer.parseInt(contractStructure[TRADING_CONTRACT_STRUCT_LOTSIZE_INDEX]);
            } catch (NumberFormatException | NullPointerException ex) {
                lotSize = 0;
            }            
        }
        return (lotSize);
    }
    
    public String getTradingContractType() {
        
        String contractType = "FUT"; // Default value
        String[] contractStructure = this.getTradingContractStructure().split("_");
        if (contractStructure.length >= TRADING_CONTRACT_STRUCT_TYPE_INDEX) {
            contractType = contractStructure[TRADING_CONTRACT_STRUCT_TYPE_INDEX];
        }
        return (contractType);
    }        

    public String getTradingContractOptionRightType() {
        
        String optionRightType = "CALL"; // Default value
        String[] contractStructure = this.getTradingContractStructure().split("_");
        if (contractStructure.length >= TRADING_CONTRACT_STRUCT_OPTIONRIGHT_INDEX) {
            optionRightType = contractStructure[TRADING_CONTRACT_STRUCT_OPTIONRIGHT_INDEX];
        }
        return (optionRightType);
    }    

    public double getTradingContractOptionStrike() {
        
        double optionStrike = 0.0; // Default value
        String[] contractStructure = this.getTradingContractStructure().split("_");
        if (contractStructure.length >= TRADING_CONTRACT_STRUCT_OPTIONSTRIKE_INDEX) {          
            try {
                optionStrike = Double.parseDouble(contractStructure[TRADING_CONTRACT_STRUCT_OPTIONSTRIKE_INDEX]);
            } catch (NumberFormatException | NullPointerException ex) {
                optionStrike = 0.0;
            }          
        }
        return (optionStrike);
    }
    
    public String getTradingContractEntryStdDev() {
        return (tradeObjectStructure[MONITORING_CONTRACT_ENTRY_STDDEV_INDEX]);
    }    
    
    public String getMonitoringContractStructure() {
        // contractStructure is <underlyingSymbol>_<contractType>
        // e.g. SBIN_STK
        // e.g. NIFTY50_FUT
        // e.g. NIFTY50_IND        
        //MONITORING_CONTRACT_STRUCT_UNDERLYING_INDEX = 0;
        //MONITORING_CONTRACT_STRUCT_TYPE_INDEX = 1;
        
        return (tradeObjectStructure[MONITORING_CONTRACT_STRUCTURE_INDEX]);
    }   

    public String getMonitroringContractUnderlyingName() {
        
        String underlyingName = "UNDERLYING"; // Default value
        String[] contractStructure = this.getMonitoringContractStructure().split("_");
        if (contractStructure.length >= MONITORING_CONTRACT_STRUCT_UNDERLYING_INDEX) {
            underlyingName = contractStructure[MONITORING_CONTRACT_STRUCT_UNDERLYING_INDEX];
        }
        return (underlyingName);
    }
    
    public String getMonitoringContractType() {
        
        String contractType = "IND"; // Default value
        String[] contractStructure = this.getMonitoringContractStructure().split("_");
        if (contractStructure.length >= MONITORING_CONTRACT_STRUCT_TYPE_INDEX) {
            contractType = contractStructure[MONITORING_CONTRACT_STRUCT_TYPE_INDEX];
        }
        return (contractType);
    }
    
    public String getMonitoringContractEntryStdDev() {
        return (tradeObjectStructure[MONITORING_CONTRACT_ENTRY_STDDEV_INDEX]);
    }

    public String getTimeSlotSubscriptionDetails() {
        return (tradeObjectStructure[TIMESLOT_SUBSCRIPTION_INDEX]);
    }
    
    public String getOrderState() {
        return (tradeObjectStructure[ORDER_STATE_INDEX]);
    }

    public String getMonitoringContractEntryPrice() {
        return (tradeObjectStructure[MONITORING_CONTRACT_ENTRY_PRICE_INDEX]);
    }    

    public String getMonitoringContractLastKnownPrice() {
        return (tradeObjectStructure[MONITORING_CONTRACT_LAST_KNOWN_PRICE_INDEX]);
    } 
    
    public String getMonitoringContractLastUpdatedTimeStamp() {
        return (tradeObjectStructure[MONITORING_CONTRACT_LAST_UPDATED_TIMESTAMP_INDEX]);
    }

    public String getMonitoringContractLowerBreach() {
        return (tradeObjectStructure[MONITORING_CONTRACT_LOWER_BREACH_INDEX]);
    }

    public String getMonitoringContractUpperBreach() {
        return (tradeObjectStructure[MONITORING_CONTRACT_UPPER_BREACH_INDEX]);
    }
    
    public String getExpiry() {
        return (tradeObjectStructure[TRADING_CONTRACT_EXPIRY_INDEX]);
    }    
    
    public String getTradingContractExpiry() {
        return (tradeObjectStructure[TRADING_CONTRACT_EXPIRY_INDEX]);
    }

    public String getTradingContractEntrySpread() {
        return (tradeObjectStructure[TRADING_CONTRACT_ENTRY_SPREAD_INDEX]);
    }
    
    public String getTradingContractEntryBidAskFillDetails() {
        return (tradeObjectStructure[TRADING_CONTRACT_ENTRY_BID_ASK_FILL_INDEX]);
    }

    public String getTradingContractEntryOrderIDs() {
        return (tradeObjectStructure[TRADING_CONTRACT_ENTRY_ORDERIDS_INDEX]);
    }

    public String getTradingContractLastKnownSpread() {
        return (tradeObjectStructure[TRADING_CONTRACT_LAST_KNOWN_SPREAD_INDEX]);
    }

    public String getTradingContractLastUpdatedTimeStamp() {
        return (tradeObjectStructure[TRADING_CONTRACT_LAST_UPDATED_TIMESTAMP_INDEX]);
    }

    public String getTradingContractExitReason() {
        return (tradeObjectStructure[TRADING_CONTRACT_EXIT_REASON_INDEX]);
    }    

    public String getTradingContractExitSpread() {
        return (tradeObjectStructure[TRADING_CONTRACT_EXIT_SPREAD_INDEX]);
    }

    public String getTradingContractExitTimeStamp() {
        return (tradeObjectStructure[TRADING_CONTRACT_EXIT_TIMESTAMP_INDEX]);
    }

    public String getTradingContractExitOrderIDs() {
        return (tradeObjectStructure[TRADING_CONTRACT_EXIT_ORDERIDS_INDEX]);
    }

    public String getTradingContractExitBidAskFillDetails() {
        return (tradeObjectStructure[TRADING_CONTRACT_EXIT_BID_ASK_FILL_INDEX]);
    }

}
